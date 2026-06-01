package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed static/*
var staticFiles embed.FS

type simulationRequest struct {
	Stock        int    `json:"stock"`
	RequestCount int    `json:"requestCount"`
	UserID       string `json:"userId"`
}

type orderRecord struct {
	OrderID   string `json:"orderId"`
	UserID    string `json:"userId"`
	CreatedAt string `json:"createdAt"`
}

type queuedOrder struct {
	OrderID    string `json:"orderId"`
	UserID     string `json:"userId"`
	AcceptedAt string `json:"acceptedAt"`
}

type demoState struct {
	initialStock int
	stock        int32
	ordersMu     sync.Mutex
	orders       []orderRecord
	failuresMu   sync.Mutex
	failures     []string
}

type asyncState struct {
	stock           int
	acceptedUsers   map[string]struct{}
	acceptedOrders  []queuedOrder
	persistedOrders []queuedOrder
}

type server struct {
	userLocks sync.Map

	asyncMu    sync.Mutex
	asyncState asyncState
	asyncQueue chan queuedOrder
}

func main() {
	srv := &server{
		asyncState: asyncState{
			stock:         5,
			acceptedUsers: map[string]struct{}{},
		},
		asyncQueue: make(chan queuedOrder, 128),
	}

	go srv.drainAsyncOrders()

	staticRoot, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatal(err)
	}
	fileServer := http.FileServer(http.FS(staticRoot))
	http.Handle("/", fileServer)
	http.HandleFunc("/api/simulate/naive-oversell", srv.handleNaiveOversell)
	http.HandleFunc("/api/simulate/optimistic-stock", srv.handleOptimisticStock)
	http.HandleFunc("/api/simulate/duplicate-order", srv.handleDuplicateOrder)
	http.HandleFunc("/api/simulate/one-person-one-order", srv.handleOnePersonOneOrder)
	http.HandleFunc("/api/async/reset", srv.handleAsyncReset)
	http.HandleFunc("/api/async/order", srv.handleAsyncOrder)
	http.HandleFunc("/api/async/state", srv.handleAsyncState)

	log.Println("Go demo running on http://localhost:8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}

func (s *server) handleNaiveOversell(w http.ResponseWriter, r *http.Request) {
	req := decodeBody(w, r)
	if req == nil {
		return
	}
	writeJSON(w, s.simulateNaiveOversell(req.Stock, req.RequestCount))
}

func (s *server) handleOptimisticStock(w http.ResponseWriter, r *http.Request) {
	req := decodeBody(w, r)
	if req == nil {
		return
	}
	writeJSON(w, s.simulateOptimisticStock(req.Stock, req.RequestCount))
}

func (s *server) handleDuplicateOrder(w http.ResponseWriter, r *http.Request) {
	req := decodeBody(w, r)
	if req == nil {
		return
	}
	writeJSON(w, s.simulateDuplicateOrdersWithoutUserLock(req.Stock, req.RequestCount, req.UserID))
}

func (s *server) handleOnePersonOneOrder(w http.ResponseWriter, r *http.Request) {
	req := decodeBody(w, r)
	if req == nil {
		return
	}
	writeJSON(w, s.simulateOnePersonOneOrderWithUserLock(req.Stock, req.RequestCount, req.UserID))
}

func (s *server) handleAsyncReset(w http.ResponseWriter, r *http.Request) {
	req := decodeBody(w, r)
	if req == nil {
		return
	}
	s.asyncMu.Lock()
	s.asyncState = asyncState{
		stock:         req.Stock,
		acceptedUsers: map[string]struct{}{},
	}
	clearChannel(s.asyncQueue)
	state := s.snapshotAsyncStateLocked()
	s.asyncMu.Unlock()
	writeJSON(w, state)
}

func (s *server) handleAsyncOrder(w http.ResponseWriter, r *http.Request) {
	req := decodeBody(w, r)
	if req == nil {
		return
	}
	writeJSON(w, s.submitAsyncOrder(req.UserID))
}

func (s *server) handleAsyncState(w http.ResponseWriter, r *http.Request) {
	s.asyncMu.Lock()
	state := s.snapshotAsyncStateLocked()
	s.asyncMu.Unlock()
	writeJSON(w, state)
}

func (s *server) simulateNaiveOversell(stock, requestCount int) map[string]any {
	state := &demoState{initialStock: stock, stock: int32(stock)}
	s.runConcurrently(requestCount, func(requestNo int) {
		observed := atomic.LoadInt32(&state.stock)
		if observed <= 0 {
			state.addFailure(fmt.Sprintf("request-%d: no stock", requestNo))
			return
		}
		time.Sleep(15 * time.Millisecond)
		atomic.StoreInt32(&state.stock, observed-1)
		time.Sleep(5 * time.Millisecond)
		state.addOrder(orderRecord{
			OrderID:   "order-" + strconv.Itoa(requestNo),
			UserID:    "user-" + strconv.Itoa(requestNo),
			CreatedAt: time.Now().Format(time.RFC3339Nano),
		})
	})
	return s.result(
		"naive-oversell",
		"先查库存，再按旧值回写，故意放大并发窗口后就会出现超卖。",
		stock,
		requestCount,
		state,
	)
}

func (s *server) simulateOptimisticStock(stock, requestCount int) map[string]any {
	state := &demoState{initialStock: stock, stock: int32(stock)}
	s.runConcurrently(requestCount, func(requestNo int) {
		if !state.decrementIfAvailable() {
			state.addFailure(fmt.Sprintf("request-%d: no stock", requestNo))
			return
		}
		state.addOrder(orderRecord{
			OrderID:   "order-" + strconv.Itoa(requestNo),
			UserID:    "user-" + strconv.Itoa(requestNo),
			CreatedAt: time.Now().Format(time.RFC3339Nano),
		})
	})
	return s.result(
		"optimistic-stock",
		"用 stock > 0 的原子条件扣减来模拟数据库乐观锁，不会超卖。",
		stock,
		requestCount,
		state,
	)
}

func (s *server) simulateDuplicateOrdersWithoutUserLock(stock, requestCount int, userID string) map[string]any {
	state := &demoState{initialStock: stock, stock: int32(stock)}
	s.runConcurrently(requestCount, func(requestNo int) {
		if state.hasOrder(userID) {
			state.addFailure(fmt.Sprintf("request-%d: duplicate blocked", requestNo))
			return
		}
		time.Sleep(15 * time.Millisecond)
		if !state.decrementIfAvailable() {
			state.addFailure(fmt.Sprintf("request-%d: no stock", requestNo))
			return
		}
		state.addOrder(orderRecord{
			OrderID:   "order-" + strconv.Itoa(requestNo),
			UserID:    userID,
			CreatedAt: time.Now().Format(time.RFC3339Nano),
		})
	})
	return s.result(
		"duplicate-order",
		"先查有没有订单，再创建订单，但这两个动作不是原子操作，所以同一用户会重复下单。",
		stock,
		requestCount,
		state,
	)
}

func (s *server) simulateOnePersonOneOrderWithUserLock(stock, requestCount int, userID string) map[string]any {
	state := &demoState{initialStock: stock, stock: int32(stock)}
	lock := s.lockForUser(userID)
	s.runConcurrently(requestCount, func(requestNo int) {
		lock.Lock()
		defer lock.Unlock()
		if state.hasOrder(userID) {
			state.addFailure(fmt.Sprintf("request-%d: duplicate blocked", requestNo))
			return
		}
		if !state.decrementIfAvailable() {
			state.addFailure(fmt.Sprintf("request-%d: no stock", requestNo))
			return
		}
		state.addOrder(orderRecord{
			OrderID:   "order-" + strconv.Itoa(requestNo),
			UserID:    userID,
			CreatedAt: time.Now().Format(time.RFC3339Nano),
		})
	})
	return s.result(
		"one-person-one-order",
		"对同一个 userId 加锁，让查询订单、扣库存、创建订单变成一个串行临界区。",
		stock,
		requestCount,
		state,
	)
}

func (s *server) submitAsyncOrder(userID string) map[string]any {
	s.asyncMu.Lock()
	defer s.asyncMu.Unlock()

	if s.asyncState.stock <= 0 {
		return map[string]any{
			"success": false,
			"message": "库存不足",
			"state":   s.snapshotAsyncStateLocked(),
		}
	}
	if _, exists := s.asyncState.acceptedUsers[userID]; exists {
		return map[string]any{
			"success": false,
			"message": "重复下单",
			"state":   s.snapshotAsyncStateLocked(),
		}
	}

	s.asyncState.stock--
	s.asyncState.acceptedUsers[userID] = struct{}{}
	order := queuedOrder{
		OrderID:    fmt.Sprintf("async-%d", time.Now().UnixNano()),
		UserID:     userID,
		AcceptedAt: time.Now().Format(time.RFC3339Nano),
	}
	s.asyncState.acceptedOrders = append(s.asyncState.acceptedOrders, order)
	s.asyncQueue <- order

	return map[string]any{
		"success": true,
		"message": "抢购成功，订单异步落库中",
		"orderId": order.OrderID,
		"state":   s.snapshotAsyncStateLocked(),
	}
}

func (s *server) drainAsyncOrders() {
	for order := range s.asyncQueue {
		time.Sleep(400 * time.Millisecond)
		s.asyncMu.Lock()
		s.asyncState.persistedOrders = append(s.asyncState.persistedOrders, order)
		s.asyncMu.Unlock()
	}
}

func (s *server) snapshotAsyncStateLocked() map[string]any {
	users := make([]string, 0, len(s.asyncState.acceptedUsers))
	for userID := range s.asyncState.acceptedUsers {
		users = append(users, userID)
	}
	return map[string]any{
		"stockLeft":       s.asyncState.stock,
		"acceptedUsers":   users,
		"acceptedOrders":  append([]queuedOrder(nil), s.asyncState.acceptedOrders...),
		"persistedOrders": append([]queuedOrder(nil), s.asyncState.persistedOrders...),
		"queueSize":       len(s.asyncQueue),
	}
}

func (s *server) result(scenario, explanation string, stock, requestCount int, state *demoState) map[string]any {
	orders := state.copyOrders()
	uniqueUsers := map[string]struct{}{}
	for _, order := range orders {
		uniqueUsers[order.UserID] = struct{}{}
	}
	return map[string]any{
		"scenario":       scenario,
		"explanation":    explanation,
		"initialStock":   stock,
		"requestCount":   requestCount,
		"successOrders":  len(orders),
		"finalStock":     int(atomic.LoadInt32(&state.stock)),
		"oversold":       len(orders) > stock,
		"duplicateOrder": len(uniqueUsers) < len(orders),
		"orders":         orders,
		"failures":       state.copyFailures(),
	}
}

func (s *server) runConcurrently(requestCount int, worker func(requestNo int)) {
	var ready sync.WaitGroup
	var start sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(requestCount)
	start.Add(1)
	done.Add(requestCount)

	for i := 0; i < requestCount; i++ {
		requestNo := i + 1
		go func() {
			ready.Done()
			start.Wait()
			worker(requestNo)
			done.Done()
		}()
	}

	ready.Wait()
	start.Done()
	done.Wait()
}

func (s *server) lockForUser(userID string) *sync.Mutex {
	lock, _ := s.userLocks.LoadOrStore(userID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (d *demoState) decrementIfAvailable() bool {
	for {
		current := atomic.LoadInt32(&d.stock)
		if current <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&d.stock, current, current-1) {
			return true
		}
	}
}

func (d *demoState) hasOrder(userID string) bool {
	d.ordersMu.Lock()
	defer d.ordersMu.Unlock()
	for _, order := range d.orders {
		if order.UserID == userID {
			return true
		}
	}
	return false
}

func (d *demoState) addOrder(order orderRecord) {
	d.ordersMu.Lock()
	d.orders = append(d.orders, order)
	d.ordersMu.Unlock()
}

func (d *demoState) copyOrders() []orderRecord {
	d.ordersMu.Lock()
	defer d.ordersMu.Unlock()
	return append([]orderRecord(nil), d.orders...)
}

func (d *demoState) addFailure(msg string) {
	d.failuresMu.Lock()
	d.failures = append(d.failures, msg)
	d.failuresMu.Unlock()
}

func (d *demoState) copyFailures() []string {
	d.failuresMu.Lock()
	defer d.failuresMu.Unlock()
	return append([]string(nil), d.failures...)
}

func decodeBody(w http.ResponseWriter, r *http.Request) *simulationRequest {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}
	var req simulationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}
	if req.UserID == "" {
		req.UserID = "user-1001"
	}
	return &req
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func clearChannel(ch chan queuedOrder) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
