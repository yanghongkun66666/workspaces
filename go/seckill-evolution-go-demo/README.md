# seckill-evolution-go-demo

一个和 Java 版保持相同教学结构的 Go 秒杀演进示例。

## 目标

- 用 Go 代码复现朴素扣库存导致的超卖
- 展示 `stock > 0` 条件扣减思想如何避免超卖
- 展示为什么不加锁会出现同一用户重复下单
- 用用户粒度锁解决一人一单
- 用异步消费者模拟“主线程快速返回、后台再落库”

## 运行

```bash
cd /home/yhk/workspaces/go/seckill-evolution-go-demo
go run .
```

启动后打开：

- [http://localhost:8090](http://localhost:8090)

## 重点代码

- `main.go`
  - 所有 HTTP 接口
  - 所有并发实验
  - 异步消费者
- `static/index.html`
  - 直接点按钮看实验结果

## 推荐阅读路径

1. `simulateNaiveOversell`
2. `simulateOptimisticStock`
3. `simulateDuplicateOrdersWithoutUserLock`
4. `simulateOnePersonOneOrderWithUserLock`
5. `submitAsyncOrder`
6. `drainAsyncOrders`

## 说明

这里依旧没有强绑 MySQL / Redis，而是先把并发问题本身用可运行的 Go 代码讲透。等你把这些实验跑熟，再映射到真实数据库条件更新、Redis 分布式锁、Lua 脚本和 MQ，会更容易建立整体理解。
