package com.sougata.natscore.model;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class PayloadWrapper<T> {

    private T payload;
    private Map<PayloadHeader, String> payloadHeaders;

    public PayloadWrapper(T payload, String payloadType) {
        this.payload = payload;
        this.payloadHeaders = new EnumMap<>(PayloadHeader.class);
        payloadHeaders.put(PayloadHeader.PAYLOAD_TYPE, payloadType);
        payloadHeaders.put(PayloadHeader.CORRELATION_ID, UUID.randomUUID().toString());
        payloadHeaders.put(PayloadHeader.CREATION_TS, String.valueOf(System.currentTimeMillis()));
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public Map<PayloadHeader, String> getPayloadHeaders() {
        return payloadHeaders;
    }

    public void setPayloadHeaders(Map<PayloadHeader, String> payloadHeaders) {
        this.payloadHeaders = payloadHeaders;
    }

    public String getHeader(PayloadHeader header) {
        return this.payloadHeaders.get(header);
    }

    public void setHeader(PayloadHeader header, String value) {
        this.payloadHeaders.put(header, value);
    }

    public boolean hasMandatoryHeaders() {
        return payloadHeaders.containsKey(PayloadHeader.PAYLOAD_TYPE) &&
                payloadHeaders.containsKey(PayloadHeader.CORRELATION_ID) &&
                payloadHeaders.containsKey(PayloadHeader.CREATION_TS);
    }

    @Override
    public String toString() {
        return "PayloadWrapper{" +
                "payload=" + payload +
                ", payloadHeaders=" + payloadHeaders +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PayloadWrapper<?> that = (PayloadWrapper<?>) o;
        return Objects.equals(payload, that.payload) &&
                Objects.equals(payloadHeaders, that.payloadHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, payloadHeaders);
    }
}