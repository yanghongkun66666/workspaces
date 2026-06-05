package com.yhk.seckilldemo;

import jakarta.annotation.PreDestroy;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class SeckillDemoService {

    private static final long VOUCHER_ID = 1L;

    private final JdbcTemplate jdbcTemplate;
    private final ExecutorService pool = Executors.newFixedThreadPool(32);
    private final Map<String, Object> userLocks = new ConcurrentHashMap<>();
    private final BlockingQueue<QueuedOrder> asyncQueue = new LinkedBlockingQueue<>();
    private final Object asyncMonitor = new Object();
    private volatile AsyncState asyncState = new AsyncState(5);
    private volatile boolean running = true;
    private final Thread consumerThread;

    public SeckillDemoService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        initializeVoucher(5);
        this.consumerThread = Thread.ofVirtual().name("async-order-consumer").start(this::drainAsyncOrders);
    }

    public Map<String, Object> simulateNaiveOversell(int stock, int requestCount) {
        resetDatabase(stock);
        List<String> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        runConcurrently(requestCount, requestNo -> {
            int observed = currentStock();
            if (observed <= 0) {
                failures.add("request-" + requestNo + ": no stock");
                return;
            }
            sleep(15);
            jdbcTemplate.update(
                "update seckill_voucher set stock = ? where voucher_id = ?",
                observed - 1, VOUCHER_ID
            );
            sleep(5);
            insertOrder("order-" + requestNo, "user-" + requestNo);
        });
        return result(
            "naive-oversell",
            "真实数据库版：先 select 库存，再按旧值 update 回写，所以并发下会超卖。",
            stock,
            requestCount,
            failures
        );
    }

    public Map<String, Object> simulateOptimisticStock(int stock, int requestCount) {
        resetDatabase(stock);
        List<String> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        runConcurrently(requestCount, requestNo -> {
            int updated = jdbcTemplate.update(
                "update seckill_voucher set stock = stock - 1 where voucher_id = ? and stock > 0",
                VOUCHER_ID
            );
            if (updated == 0) {
                failures.add("request-" + requestNo + ": no stock");
                return;
            }
            insertOrder("order-" + requestNo, "user-" + requestNo);
        });
        return result(
            "optimistic-stock",
            "真实数据库版：update ... where stock > 0，只会有库存充足的请求更新成功。",
            stock,
            requestCount,
            failures
        );
    }

    public Map<String, Object> simulateDuplicateOrdersWithoutUserLock(int stock, int requestCount, String userId) {
        resetDatabase(stock);
        List<String> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        runConcurrently(requestCount, requestNo -> {
            if (hasOrder(userId)) {
                failures.add("request-" + requestNo + ": duplicate blocked");
                return;
            }
            sleep(15);
            int updated = jdbcTemplate.update(
                "update seckill_voucher set stock = stock - 1 where voucher_id = ? and stock > 0",
                VOUCHER_ID
            );
            if (updated == 0) {
                failures.add("request-" + requestNo + ": no stock");
                return;
            }
            insertOrder("order-" + requestNo, userId);
        });
        return result(
            "duplicate-order",
            "真实数据库版：先查订单再下单，但查和写不是原子操作，所以同一用户会重复下单。",
            stock,
            requestCount,
            failures
        );
    }

    public Map<String, Object> simulateOnePersonOneOrderWithUserLock(int stock, int requestCount, String userId) {
        resetDatabase(stock);
        List<String> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        runConcurrently(requestCount, requestNo -> {
            synchronized (lockForUser(userId)) {
                if (hasOrder(userId)) {
                    failures.add("request-" + requestNo + ": duplicate blocked");
                    return;
                }
                int updated = jdbcTemplate.update(
                    "update seckill_voucher set stock = stock - 1 where voucher_id = ? and stock > 0",
                    VOUCHER_ID
                );
                if (updated == 0) {
                    failures.add("request-" + requestNo + ": no stock");
                    return;
                }
                insertOrder("order-" + requestNo, userId);
            }
        });
        return result(
            "one-person-one-order",
            "真实数据库版：对同一个 userId 加锁，把查订单、扣库存、写订单包成串行临界区。",
            stock,
            requestCount,
            failures
        );
    }

    public Map<String, Object> resetAsyncDemo(int stock) {
        synchronized (asyncMonitor) {
            asyncQueue.clear();
            asyncState = new AsyncState(stock);
            resetDatabase(stock);
        }
        return asyncState();
    }

    public Map<String, Object> submitAsyncOrder(String userId) {
        synchronized (asyncMonitor) {
            if (asyncState.stock <= 0) {
                return Map.of("success", false, "message", "库存不足", "state", snapshot(asyncState));
            }
            if (asyncState.acceptedUsers.contains(userId)) {
                return Map.of("success", false, "message", "重复下单", "state", snapshot(asyncState));
            }
            asyncState.stock -= 1;
            asyncState.acceptedUsers.add(userId);
            QueuedOrder order = new QueuedOrder(UUID.randomUUID().toString(), userId, Instant.now().toString());
            asyncState.acceptedOrders.add(order);
            asyncQueue.offer(order);
            return Map.of(
                "success", true,
                "message", "抢购成功，Redis/Lua 这一步在示例里用内存队列模拟，数据库落库在后台线程执行",
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

    private void initializeVoucher(int stock) {
        jdbcTemplate.update(
            "insert into seckill_voucher (voucher_id, stock) values (?, ?) on duplicate key update stock = values(stock)",
            VOUCHER_ID, stock
        );
    }

    private void resetDatabase(int stock) {
        jdbcTemplate.update("delete from voucher_order");
        initializeVoucher(stock);
    }

    private int currentStock() {
        Integer value = jdbcTemplate.queryForObject(
            "select stock from seckill_voucher where voucher_id = ?",
            Integer.class,
            VOUCHER_ID
        );
        return value == null ? 0 : value;
    }

    private boolean hasOrder(String userId) {
        Integer count = jdbcTemplate.queryForObject(
            "select count(*) from voucher_order where voucher_id = ? and user_id = ?",
            Integer.class,
            VOUCHER_ID,
            userId
        );
        return count != null && count > 0;
    }

    private void insertOrder(String orderId, String userId) {
        jdbcTemplate.update(
            "insert into voucher_order (order_id, user_id, voucher_id, created_at) values (?, ?, ?, ?)",
            orderId, userId, VOUCHER_ID, Timestamp.from(Instant.now())
        );
    }

    private List<OrderRecord> loadOrders() {
        return jdbcTemplate.query(
            "select order_id, user_id, created_at from voucher_order order by created_at, order_id",
            (ResultSet rs, int rowNum) -> new OrderRecord(
                rs.getString("order_id"),
                rs.getString("user_id"),
                rs.getTimestamp("created_at").toInstant().toString()
            )
        );
    }

    private void drainAsyncOrders() {
        while (running) {
            try {
                QueuedOrder order = asyncQueue.take();
                sleep(400);
                jdbcTemplate.update(
                    "insert into voucher_order (order_id, user_id, voucher_id, created_at) values (?, ?, ?, ?)",
                    order.orderId(), order.userId(), VOUCHER_ID, Timestamp.from(Instant.parse(order.acceptedAt()))
                );
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

    private Map<String, Object> result(String scenario, String explanation, int stock, int requestCount, List<String> failures) {
        List<OrderRecord> orders = loadOrders();
        long uniqueUsers = orders.stream().map(OrderRecord::userId).distinct().count();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("scenario", scenario);
        body.put("explanation", explanation);
        body.put("initialStock", stock);
        body.put("requestCount", requestCount);
        body.put("successOrders", orders.size());
        body.put("finalStock", currentStock());
        body.put("oversold", orders.size() > stock);
        body.put("duplicateOrder", uniqueUsers < orders.size());
        body.put("orders", orders);
        body.put("failures", failures);
        body.put("storage", Map.of(
            "database", "H2",
            "voucherTable", "seckill_voucher",
            "orderTable", "voucher_order"
        ));
        return body;
    }

    private Map<String, Object> snapshot(AsyncState state) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("stockLeft", state.stock);
        body.put("acceptedUsers", new ArrayList<>(state.acceptedUsers));
        body.put("acceptedOrders", new ArrayList<>(state.acceptedOrders));
        body.put("persistedOrders", new ArrayList<>(state.persistedOrders));
        body.put("queueSize", asyncQueue.size());
        body.put("dbOrders", loadOrders());
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
                await(ready);
                await(start);
                try {
                    worker.accept(requestNo);
                } finally {
                    done.countDown();
                }
            });
        }
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

    private static final class AsyncState {
        private int stock;
        private final java.util.Set<String> acceptedUsers = ConcurrentHashMap.newKeySet();
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
