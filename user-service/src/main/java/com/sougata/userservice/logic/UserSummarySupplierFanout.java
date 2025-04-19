package com.sougata.userservice.logic;

import com.sougata.natscore.contract.PayloadSupplierFanout;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.userprotos.UserSummary;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

@Component
public class UserSummarySupplierFanout implements PayloadSupplierFanout {
    private final BlockingQueue<List<UserSummary>> eventQueue = new LinkedBlockingQueue<>();

    public UserSummarySupplierFanout() {
        new Thread(() -> {
            while (true) {
                try {
                    List<UserSummary> userSummaries = Arrays.asList(
                            UserSummary.newBuilder()
                                .setSummary("This is a user summary")
                                .setGeneratedAt(System.currentTimeMillis())
                                .build(),
                            UserSummary.newBuilder()
                                    .setSummary("This is another user summary")
                                    .setGeneratedAt(System.currentTimeMillis())
                                    .build()
                    );
                    eventQueue.put(userSummaries);
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "SummaryEventProducer").start();
    }

    @Override
    public List<PayloadWrapper<byte[]>> supply() {
        try {
            List<UserSummary> userSummaries = eventQueue.take();
            return userSummaries.stream().map(userSummary ->
                            PayloadWrapper.<byte[]>newBuilder()
                                    .setPayload(userSummary.toByteArray())
                                    .setPayloadType(UserSummary.class.getName())
                                    .build()
                    ).collect(Collectors.toList());
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for summary event", e);
        }
    }
}

