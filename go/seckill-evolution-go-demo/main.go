package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed static/*
var staticFiles embed.FS

const voucherID int64 = 1

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

type asyncState struct {
	stock           int
	acceptedUsers   map[string]struct{}
	acceptedOrders  []queuedOrder
	persistedOrders []queuedOrder
}

type server struct {
	db *sql.DB

	userLocks sync.Map

	asyncMu    sync.Mutex
	asyncState asyncState
	asyncQueue chan queuedOrder
}

func main() {
	db, err := sql.Open("mysql", mysqlDSN())
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	if err := initDB(db); err != nil {
		log.Fatal(err)
	}

	srv := &server{
		db: db,
		asyncState: asyncState{
			stock:         5,
			acceptedUsers: map[string]struct{}{},
		},
		asyncQueue: make(chan queuedOrder, 128),
	}
	if err := srv.resetDatabase(5); err != nil {
		log.Fatal(err)
	}

	go srv.drainAsyncOrders()

	staticRoot, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatal(err)
	}
	fileServer := http.FileServer(http.FS(staticRoot))
	http.Handle("/", fileServer)
	http.HandleFunc("/api/simulate/naive-oversell", srv.withCORS(srv.handleNaiveOversell))
	http.HandleFunc("/api/simulate/optimistic-stock", srv.withCORS(srv.handleOptimisticStock))
	http.HandleFunc("/api/simulate/duplicate-order", srv.withCORS(srv.handleDuplicateOrder))
	http.HandleFunc("/api/simulate/one-person-one-order", srv.withCORS(srv.handleOnePersonOneOrder))
	http.HandleFunc("/api/async/reset", srv.withCORS(srv.handleAsyncReset))
	http.HandleFunc("/api/async/order", srv.withCORS(srv.handleAsyncOrder))
	http.HandleFunc("/api/async/state", srv.withCORS(srv.handleAsyncState))

	log.Println("Go demo running on http://localhost:8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}

func initDB(db *sql.DB) error {
	stmts := []string{
		"CREATE TABLE IF NOT EXISTS seckill_voucher (voucher_id BIGINT PRIMARY KEY, stock INT NOT NULL)",
		"CREATE TABLE IF NOT EXISTS voucher_order (order_id VARCHAR(64) PRIMARY KEY, user_id VARCHAR(64) NOT NULL, voucher_id BIGINT NOT NULL, created_at TIMESTAMP NOT NULL, KEY idx_voucher_order_user_voucher (user_id, voucher_id))",
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
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
	_ = s.resetDatabase(req.Stock)
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
	_ = s.resetDatabase(stock)
	var failures []string
	var failuresMu sync.Mutex
	s.runConcurrently(requestCount, func(requestNo int) {
		observed := s.currentStock()
		if observed <= 0 {
			failuresMu.Lock()
			failures = append(failures, fmt.Sprintf("request-%d: no stock", requestNo))
			failuresMu.Unlock()
			return
		}
		time.Sleep(15 * time.Millisecond)
		_, _ = s.db.Exec("UPDATE seckill_voucher SET stock = ? WHERE voucher_id = ?", observed-1, voucherID)
		time.Sleep(5 * time.Millisecond)
		_ = s.insertOrder(fmt.Sprintf("order-%d", requestNo), fmt.Sprintf("user-%d", requestNo))
	})
	return s.result("naive-oversell", "真实数据库版：先 select 库存，再按旧值 update 回写，所以并发下会超卖。", stock, requestCount, failures)
}

func (s *server) simulateOptimisticStock(stock, requestCount int) map[string]any {
	_ = s.resetDatabase(stock)
	var failures []string
	var failuresMu sync.Mutex
	s.runConcurrently(requestCount, func(requestNo int) {
		ok := s.decrementStockOptimistic()
		if !ok {
			failuresMu.Lock()
			failures = append(failures, fmt.Sprintf("request-%d: no stock", requestNo))
			failuresMu.Unlock()
			return
		}
		_ = s.insertOrder(fmt.Sprintf("order-%d", requestNo), fmt.Sprintf("user-%d", requestNo))
	})
	return s.result("optimistic-stock", "真实数据库版：update ... where stock > 0，只会有库存充足的请求更新成功。", stock, requestCount, failures)
}

func (s *server) simulateDuplicateOrdersWithoutUserLock(stock, requestCount int, userID string) map[string]any {
	_ = s.resetDatabase(stock)
	var failures []string
	var failuresMu sync.Mutex
	s.runConcurrently(requestCount, func(requestNo int) {
		if s.hasOrder(userID) {
			failuresMu.Lock()
			failures = append(failures, fmt.Sprintf("request-%d: duplicate blocked", requestNo))
			failuresMu.Unlock()
			return
		}
		time.Sleep(15 * time.Millisecond)
		if !s.decrementStockOptimistic() {
			failuresMu.Lock()
			failures = append(failures, fmt.Sprintf("request-%d: no stock", requestNo))
			failuresMu.Unlock()
			return
		}
		_ = s.insertOrder(fmt.Sprintf("order-%d", requestNo), userID)
	})
	return s.result("duplicate-order", "真实数据库版：先查订单再下单，但查和写不是原子操作，所以同一用户会重复下单。", stock, requestCount, failures)
}

func (s *server) simulateOnePersonOneOrderWithUserLock(stock, requestCount int, userID string) map[string]any {
	_ = s.resetDatabase(stock)
	var failures []string
	var failuresMu sync.Mutex
	lock := s.lockForUser(userID)
	s.runConcurrently(requestCount, func(requestNo int) {
		lock.Lock()
		defer lock.Unlock()
		if s.hasOrder(userID) {
			failuresMu.Lock()
			failures = append(failures, fmt.Sprintf("request-%d: duplicate blocked", requestNo))
			failuresMu.Unlock()
			return
		}
		if !s.decrementStockOptimistic() {
			failuresMu.Lock()
			failures = append(failures, fmt.Sprintf("request-%d: no stock", requestNo))
			failuresMu.Unlock()
			return
		}
		_ = s.insertOrder(fmt.Sprintf("order-%d", requestNo), userID)
	})
	return s.result("one-person-one-order", "真实数据库版：对同一个 userId 加锁，把查订单、扣库存、写订单包成串行临界区。", stock, requestCount, failures)
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
		"message": "抢购成功，队列模拟 Redis Stream / MQ，数据库落库在后台 goroutine 执行",
		"orderId": order.OrderID,
		"state":   s.snapshotAsyncStateLocked(),
	}
}

func (s *server) drainAsyncOrders() {
	for order := range s.asyncQueue {
		time.Sleep(400 * time.Millisecond)
		_ = s.insertOrder(order.OrderID, order.UserID)
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
		"dbOrders":        s.loadOrders(),
	}
}

func (s *server) result(scenario, explanation string, stock, requestCount int, failures []string) map[string]any {
	orders := s.loadOrders()
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
		"finalStock":     s.currentStock(),
		"oversold":       len(orders) > stock,
		"duplicateOrder": len(uniqueUsers) < len(orders),
		"orders":         orders,
		"failures":       failures,
		"storage": map[string]any{
			"database":     "SQLite",
			"voucherTable": "seckill_voucher",
			"orderTable":   "voucher_order",
		},
	}
}

func (s *server) resetDatabase(stock int) error {
	if _, err := s.db.Exec("DELETE FROM voucher_order"); err != nil {
		return err
	}
	if _, err := s.db.Exec("INSERT INTO seckill_voucher (voucher_id, stock) VALUES (?, ?) ON DUPLICATE KEY UPDATE stock = VALUES(stock)", voucherID, stock); err != nil {
		return err
	}
	return nil
}

func (s *server) currentStock() int {
	var stock int
	if err := s.db.QueryRow("SELECT stock FROM seckill_voucher WHERE voucher_id = ?", voucherID).Scan(&stock); err != nil {
		return 0
	}
	return stock
}

func (s *server) hasOrder(userID string) bool {
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM voucher_order WHERE voucher_id = ? AND user_id = ?", voucherID, userID).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func (s *server) decrementStockOptimistic() bool {
	res, err := s.db.Exec("UPDATE seckill_voucher SET stock = stock - 1 WHERE voucher_id = ? AND stock > 0", voucherID)
	if err != nil {
		return false
	}
	rows, err := res.RowsAffected()
	return err == nil && rows == 1
}

func (s *server) insertOrder(orderID, userID string) error {
	_, err := s.db.Exec(
		"INSERT INTO voucher_order (order_id, user_id, voucher_id, created_at) VALUES (?, ?, ?, ?)",
		orderID, userID, voucherID, time.Now().Format(time.RFC3339Nano),
	)
	return err
}

func (s *server) loadOrders() []orderRecord {
	rows, err := s.db.Query("SELECT order_id, user_id, created_at FROM voucher_order ORDER BY created_at, order_id")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var orders []orderRecord
	for rows.Next() {
		var item orderRecord
		if err := rows.Scan(&item.OrderID, &item.UserID, &item.CreatedAt); err == nil {
			orders = append(orders, item)
		}
	}
	return orders
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
			ready.Wait()
			start.Wait()
			worker(requestNo)
			done.Done()
		}()
	}

	start.Done()
	done.Wait()
}

func (s *server) lockForUser(userID string) *sync.Mutex {
	lock, _ := s.userLocks.LoadOrStore(userID, &sync.Mutex{})
	return lock.(*sync.Mutex)
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

func mysqlDSN() string {
	host := envOr("MYSQL_HOST", "127.0.0.1")
	port := envOr("MYSQL_PORT", "3306")
	database := envOr("MYSQL_DATABASE", "seckill_demo")
	user := envOr("MYSQL_USER", "root")
	password := envOr("MYSQL_PASSWORD", "200143")
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&loc=Asia%%2FShanghai", user, password, host, port, database)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
