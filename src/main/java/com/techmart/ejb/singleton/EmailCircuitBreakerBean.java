package com.techmart.ejb.singleton;

import jakarta.ejb.Singleton;
import jakarta.ejb.Lock;
import jakarta.ejb.LockType;
import java.util.logging.Logger;

@Singleton
public class EmailCircuitBreakerBean {

    private static final Logger LOG = Logger.getLogger(EmailCircuitBreakerBean.class.getName());

    private enum State { CLOSED, OPEN, HALF_OPEN }

    private State state = State.CLOSED;
    private int failureCount = 0;
    private long lastFailureTime = 0;

    private static final int FAILURE_THRESHOLD = 3;
    private static final long RETRY_TIMEOUT_MS = 10000; // 10 seconds

    @Lock(LockType.READ)
    public boolean allowRequest() {
        if (state == State.CLOSED) {
            return true;
        }

        if (state == State.OPEN) {
            // Check if enough time has passed to try again (Half-Open)
            if (System.currentTimeMillis() - lastFailureTime > RETRY_TIMEOUT_MS) {
                return true;
            }
            return false;
        }
        return true;
    }

    @Lock(LockType.WRITE)
    public void recordSuccess() {
        failureCount = 0;
        if (state != State.CLOSED) {
            LOG.info("SMTP service recovered. Circuit Breaker is CLOSED.");
            state = State.CLOSED;
        }
    }

    @Lock(LockType.WRITE)
    public void recordFailure() {
        failureCount++;
        lastFailureTime = System.currentTimeMillis();

        if (state == State.CLOSED && failureCount >= FAILURE_THRESHOLD) {
            state = State.OPEN;
            LOG.severe("SMTP failure threshold reached. Circuit Breaker tripped to OPEN.");
        }
    }
}