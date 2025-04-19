package com.sougata.userservice.logic;

import com.sougata.natscore.contract.PayloadSupplier;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.userprotos.UserSummary;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class UserSummarySupplier implements PayloadSupplier {

    private final BlockingQueue<UserSummary> eventQueue = new LinkedBlockingQueue<>();

    public UserSummarySupplier() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(99999999);
                    UserSummary userSummary = UserSummary.newBuilder()
                            .setSummary("This is a user summary")
                            .setGeneratedAt(System.currentTimeMillis())
                            .build();
                    eventQueue.put(userSummary);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "SummaryEventProducer").start();
    }

    @Override
    public PayloadWrapper<byte[]> supply() {
        try {
            UserSummary userSummary = eventQueue.take();
            return PayloadWrapper.<byte[]>newBuilder()
                    .setPayload(userSummary.toByteArray())
                    .setPayloadType(UserSummary.class.getName())
                    .build();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for summary event", e);
        }
    }
}

