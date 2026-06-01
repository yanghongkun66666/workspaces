# seckill-evolution-java-demo

一个可运行、可读代码、可直接复现实验的 Java 秒杀演进示例。

## 你能学到什么

- `朴素扣库存` 为什么会超卖
- `stock > 0` 这种条件更新为什么能避免超卖
- 为什么 `查询订单 + 创建订单` 不是原子操作
- 为什么单 JVM 下可以用用户粒度锁解决一人一单
- 为什么异步落库可以降低主链路压力

## 项目结构

- `src/main/java/com/yhk/seckilldemo/SeckillDemoService.java`
  - 核心实验逻辑
  - 朴素版、乐观锁版、一人一单版、异步落库版都在这里
- `src/main/java/com/yhk/seckilldemo/SeckillDemoController.java`
  - 提供页面调用的 HTTP API
- `src/main/resources/static/index.html`
  - 前端页面，直接点按钮即可复现实验

## 运行

```bash
cd /home/yhk/workspaces/java/seckill-evolution-java-demo
mvn spring-boot:run
```

启动后打开：

- [http://localhost:8080](http://localhost:8080)

## 推荐实验顺序

1. `超卖实验`
   - 先点 `朴素版本`
   - 再点 `stock > 0 乐观锁版本`
2. `一人一单实验`
   - 先点 `不加锁`
   - 再点 `用户粒度锁`
3. `异步落库实验`
   - 先 reset
   - 再提交几个不同用户订单
   - 观察 `Redis 侧已受理` 和 `DB 已落库` 的时间差

## 代码与面试话术对应关系

### 1. 朴素版超卖

`simulateNaiveOversell()`

- 先读库存
- 再 sleep 放大并发窗口
- 再按旧值回写库存
- 所以可能出现 `订单数 > 初始库存`

### 2. stock > 0 乐观锁

`decrementStockIfAvailable()`

- 用同步块模拟数据库里的原子条件更新
- 对应 SQL：

```sql
update voucher
set stock = stock - 1
where voucher_id = ? and stock > 0
```

### 3. 一人一单

- `simulateDuplicateOrdersWithoutUserLock()`
  - 先查订单再创建订单，但不是原子操作，所以会重复下单
- `simulateOnePersonOneOrderWithUserLock()`
  - 用 `synchronized (lockForUser(userId))` 串行化同一个用户

### 4. 异步落库

- `submitAsyncOrder()`
  - 用单个同步块模拟 Redis Lua 脚本的原子校验
- `drainAsyncOrders()`
  - 模拟后台消费者异步写库

## 说明

这里为了让你专注理解并发问题，没有接 MySQL / Redis，而是用可控的内存模型把问题和优化过程“放大”给你看。你学懂后，再把这些逻辑映射到真实数据库、Redis、MQ 就会很顺。
