package com.yhk.seckilldemo;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class SeckillDemoController {

    private final SeckillDemoService service;

    public SeckillDemoController(SeckillDemoService service) {
        this.service = service;
    }

    @PostMapping("/simulate/naive-oversell")
    public Map<String, Object> simulateNaiveOversell(@RequestBody SimulationRequest request) {
        return service.simulateNaiveOversell(request.stock(), request.requestCount());
    }

    @PostMapping("/simulate/optimistic-stock")
    public Map<String, Object> simulateOptimisticStock(@RequestBody SimulationRequest request) {
        return service.simulateOptimisticStock(request.stock(), request.requestCount());
    }

    @PostMapping("/simulate/duplicate-order")
    public Map<String, Object> simulateDuplicateOrder(@RequestBody DuplicateOrderRequest request) {
        return service.simulateDuplicateOrdersWithoutUserLock(request.stock(), request.requestCount(), request.userId());
    }

    @PostMapping("/simulate/one-person-one-order")
    public Map<String, Object> simulateOnePersonOneOrder(@RequestBody DuplicateOrderRequest request) {
        return service.simulateOnePersonOneOrderWithUserLock(request.stock(), request.requestCount(), request.userId());
    }

    @PostMapping("/async/reset")
    public Map<String, Object> resetAsync(@RequestBody ResetRequest request) {
        return service.resetAsyncDemo(request.stock());
    }

    @PostMapping("/async/order")
    public Map<String, Object> submitAsyncOrder(@RequestBody AsyncOrderRequest request) {
        return service.submitAsyncOrder(request.userId());
    }

    @GetMapping("/async/state")
    public Map<String, Object> asyncState() {
        return service.asyncState();
    }

    public record SimulationRequest(int stock, int requestCount) {
    }

    public record DuplicateOrderRequest(int stock, int requestCount, String userId) {
    }

    public record ResetRequest(int stock) {
    }

    public record AsyncOrderRequest(String userId) {
    }
}
