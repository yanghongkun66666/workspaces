# seckill-evolution-go-demo

一个和 Java 版保持相同教学结构的 Go 秒杀演进示例，当前版本默认连接本地 MySQL。

## 目标

- 用 Go 代码复现朴素扣库存导致的超卖
- 展示 `stock > 0` 条件扣减思想如何避免超卖
- 展示为什么不加锁会出现同一用户重复下单
- 用用户粒度锁解决一人一单
- 用异步消费者模拟“主线程快速返回、后台再落库”

## 运行

先在项目目录里启动本项目专用 MySQL：

```bash
cd /home/yhk/workspaces/go/seckill-evolution-go-demo
docker compose up -d
```

再启动项目：

```bash
cd /home/yhk/workspaces/go/seckill-evolution-go-demo
MYSQL_PORT=3308 go run .
```

如果你在 GoLand 里启动，请在 Run Configuration 里添加环境变量：

```text
MYSQL_PORT=3308
MYSQL_USER=root
MYSQL_PASSWORD=200143
MYSQL_DATABASE=seckill_demo
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

当前示例已经接上真实 MySQL，所以你可以直接观察真实库存表和订单表在不同并发方案下的变化。异步落库里的 Redis / MQ 仍然使用内存队列模拟，重点是帮助你理解主链路削峰和后台落库的思路。
