package com.yhk.seckilldemo;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.stereotype.Service;

@Service
public class SeckillDemoService {

    private final ExecutorService pool = Executors.newFixedThreadPool(32);
    private final Map<String, Object> userLocks = new ConcurrentHashMap<>();
    private final BlockingQueue<QueuedOrder> asyncQueue = new LinkedBlockingQueue<>();
    private final Object asyncMonitor = new Object();
    private volatile AsyncState asyncState = new AsyncState(5);
    private volatile boolean running = true;
    private final Thread consumerThread;

    public SeckillDemoService() {
        this.consumerThread = Thread.ofVirtual().name("async-order-consumer").start(this::drainAsyncOrders);
    }

    public Map<String, Object> simulateNaiveOversell(int stock, int requestCount) {
        DemoState state = new DemoState(stock);
        runConcurrently(requestCount, requestNo -> {
            int observed = state.stock;
            if (observed <= 0) {
                state.failures.add("request-" + requestNo + ": no stock");
                return;
            }
            sleep(15);
            state.stock = observed - 1;
            sleep(5);
            state.orders.add(new OrderRecord("order-" + requestNo, "user-" + requestNo, Instant.now().toString()));
        });
        return result(
            "naive-oversell",
            "先查库存，再按旧值回写，故意放大并发窗口后就会出现超卖。",
            state.initialStock,
            requestCount,
            state
        );
    }

    public Map<String, Object> simulateOptimisticStock(int stock, int requestCount) {
        DemoState state = new DemoState(stock);
        runConcurrently(requestCount, requestNo -> {
            if (!state.decrementStockIfAvailable()) {
                state.failures.add("request-" + requestNo + ": no stock");
                return;
            }
            state.orders.add(new OrderRecord("order-" + requestNo, "user-" + requestNo, Instant.now().toString()));
        });
        return result(
            "optimistic-stock",
            "用 stock > 0 的原子条件扣减来模拟数据库乐观锁，不会超卖。",
            state.initialStock,
            requestCount,
            state
        );
    }

    public Map<String, Object> simulateDuplicateOrdersWithoutUserLock(int stock, int requestCount, String userId) {
        DemoState state = new DemoState(stock);
        runConcurrently(requestCount, requestNo -> {
            if (state.hasOrder(userId)) {
                state.failures.add("request-" + requestNo + ": duplicate blocked");
                return;
            }
            sleep(15);
            if (!state.decrementStockIfAvailable()) {
                state.failures.add("request-" + requestNo + ": no stock");
                return;
            }
            state.orders.add(new OrderRecord("order-" + requestNo, userId, Instant.now().toString()));
        });
        return result(
            "duplicate-order",
            "先查有没有订单，再创建订单，但这两个动作不是原子操作，所以同一用户会重复下单。",
            state.initialStock,
            requestCount,
            state
        );
    }

    public Map<String, Object> simulateOnePersonOneOrderWithUserLock(int stock, int requestCount, String userId) {
        DemoState state = new DemoState(stock);
        runConcurrently(requestCount, requestNo -> {
            synchronized (lockForUser(userId)) {
                if (state.hasOrder(userId)) {
                    state.failures.add("request-" + requestNo + ": duplicate blocked");
                    return;
                }
                if (!state.decrementStockIfAvailable()) {
                    state.failures.add("request-" + requestNo + ": no stock");
                    return;
                }
                state.orders.add(new OrderRecord("order-" + requestNo, userId, Instant.now().toString()));
            }
        });
        return result(
            "one-person-one-order",
            "对同一个 userId 加锁，让查询订单、扣库存、创建订单变成一个串行临界区。",
            state.initialStock,
            requestCount,
            state
        );
    }

    public Map<String, Object> resetAsyncDemo(int stock) {
        synchronized (asyncMonitor) {
            asyncQueue.clear();
            asyncState = new AsyncState(stock);
        }
        return asyncState();
    }

    public Map<String, Object> submitAsyncOrder(String userId) {
        synchronized (asyncMonitor) {
            if (asyncState.stock <= 0) {
                return Map.of(
                    "success", false,
                    "message", "库存不足",
                    "state", snapshot(asyncState)
                );
            }
            if (asyncState.acceptedUsers.contains(userId)) {
                return Map.of(
                    "success", false,
                    "message", "重复下单",
                    "state", snapshot(asyncState)
                );
            }
            asyncState.stock -= 1;
            asyncState.acceptedUsers.add(userId);
            QueuedOrder order = new QueuedOrder(UUID.randomUUID().toString(), userId, Instant.now().toString());
            asyncState.acceptedOrders.add(order);
            asyncQueue.offer(order);
            return Map.of(
                "success", true,
                "message", "抢购成功，订单异步落库中",
                "orderId", order.orderId(),
                "state", snapshot(asyncState)
            );
        }
    }

    public Map<String, Object> asyncState() {
        synchronized (asyncMonitor) {
            return snapshot(asyncState);
        }
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        consumerThread.interrupt();
        pool.shutdownNow();
    }

    private void drainAsyncOrders() {
        while (running) {
            try {
                QueuedOrder order = asyncQueue.take();
                sleep(400);
                synchronized (asyncMonitor) {
                    asyncState.persistedOrders.add(order);
                }
            } catch (InterruptedException ignored) {
                if (!running) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private Map<String, Object> result(String scenario, String explanation, int stock, int requestCount, DemoState state) {
        long uniqueUsers = state.orders.stream().map(OrderRecord::userId).distinct().count();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("scenario", scenario);
        body.put("explanation", explanation);
        body.put("initialStock", stock);
        body.put("requestCount", requestCount);
        body.put("successOrders", state.orders.size());
        body.put("finalStock", state.stock);
        body.put("oversold", state.orders.size() > stock);
        body.put("duplicateOrder", uniqueUsers < state.orders.size());
        body.put("orders", state.orders);
        body.put("failures", state.failures);
        return body;
    }

    private Map<String, Object> snapshot(AsyncState state) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("stockLeft", state.stock);
        body.put("acceptedUsers", new ArrayList<>(state.acceptedUsers));
        body.put("acceptedOrders", new ArrayList<>(state.acceptedOrders));
        body.put("persistedOrders", new ArrayList<>(state.persistedOrders));
        body.put("queueSize", asyncQueue.size());
        return body;
    }

    private Object lockForUser(String userId) {
        return userLocks.computeIfAbsent(userId, key -> new Object());
    }

    private void runConcurrently(int requestCount, CheckedConsumer worker) {
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(requestCount);
        for (int i = 0; i < requestCount; i++) {
            final int requestNo = i + 1;
            pool.submit(() -> {
                ready.countDown();
                await(start);
                try {
                    worker.accept(requestNo);
                } finally {
                    done.countDown();
                }
            });
        }
        await(ready);
        start.countDown();
        await(done);
    }

    private void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted", e);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted", e);
        }
    }

    @FunctionalInterface
    private interface CheckedConsumer {
        void accept(int requestNo);
    }

    private static final class DemoState {
        private final int initialStock;
        private final List<OrderRecord> orders = Collections.synchronizedList(new ArrayList<>());
        private final List<String> failures = Collections.synchronizedList(new ArrayList<>());
        private int stock;

        private DemoState(int stock) {
            this.initialStock = stock;
            this.stock = stock;
        }

        private boolean decrementStockIfAvailable() {
            synchronized (this) {
                if (stock <= 0) {
                    return false;
                }
                stock -= 1;
                return true;
            }
        }

        private boolean hasOrder(String userId) {
            return orders.stream().anyMatch(order -> order.userId().equals(userId));
        }
    }

    private static final class AsyncState {
        private int stock;
        private final Set<String> acceptedUsers = ConcurrentHashMap.newKeySet();
        private final List<QueuedOrder> acceptedOrders = new ArrayList<>();
        private final List<QueuedOrder> persistedOrders = new ArrayList<>();

        private AsyncState(int stock) {
            this.stock = stock;
        }
    }

    public record OrderRecord(String orderId, String userId, String createdAt) {
    }

    public record QueuedOrder(String orderId, String userId, String acceptedAt) {
    }
}
