package com.sougata.userservice.logic;

import com.sougata.natscore.contract.PayloadSupplier;
import com.sougata.natscore.model.PayloadWrapper;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class UserSummarySupplierLogic implements PayloadSupplier {

    private final BlockingQueue<String> eventQueue = new LinkedBlockingQueue<>();

    public UserSummarySupplierLogic() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(7200000);
                    String summary = "Summary at " + Instant.now();
                    eventQueue.put(summary);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "SummaryEventProducer").start();
    }

    @Override
    public PayloadWrapper<byte[]> supply() {
        try {
            String data = eventQueue.take();
            return new PayloadWrapper<>(data.getBytes(StandardCharsets.UTF_8), "com.sougata.protos.UserSummary");
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for summary event", e);
        }
    }
}

