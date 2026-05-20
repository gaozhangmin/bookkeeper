/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.proto;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Controls memory back-pressure for in-flight requests.
 *
 * <p>Tracks the total bytes currently occupied by in-flight requests (e.g. write or read).
 * When the tracked bytes exceed the configured limit, new requests are rejected
 * immediately with {@code ETOOMANYREQUESTS}. A limit of {@code 0} disables the check.
 */
public class MemoryLimitController {

    /** Total bytes of requests currently in-flight. */
    private final AtomicLong bytesInProgress = new AtomicLong(0);

    /** Maximum total bytes allowed for in-flight requests. 0 means unlimited. */
    private final long maxBytesLimit;

    public MemoryLimitController(long maxBytesLimit, String gaugeKey, StatsLogger statsLogger) {
        this.maxBytesLimit = maxBytesLimit;
        registerGauges(gaugeKey, statsLogger);
    }

    private void registerGauges(String gaugeKey, StatsLogger statsLogger) {
        statsLogger.registerGauge(gaugeKey, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return bytesInProgress.get();
            }
        });
    }

    /**
     * Attempts to account {@code bytes} for a new request.
     *
     * <p>The bytes are always added to {@link #bytesInProgress} (for monitoring).
     * If the limit is enabled ({@code > 0}) and the new total would exceed it,
     * the bytes are subtracted back and {@code false} is returned (request should be rejected).
     * Otherwise {@code true} is returned (request is accepted and bytes are accounted).
     *
     * @param bytes the size of the new request
     * @return {@code true} if accepted and accounted; {@code false} if the request should be rejected
     */
    public boolean tryAcquireBytes(long bytes) {
        while (true) {
            long current = bytesInProgress.get();
            long newTotal = current + bytes;
            if (newTotal > maxBytesLimit) {
                return false; // over limit → reject
            }
            if (bytesInProgress.compareAndSet(current, newTotal)) {
                return true; // accepted and accounted
            }
        }
    }

    /**
     * Accounts {@code bytes} for an accepted request.
     * Must be paired with a later call to {@link #releaseBytes(long)}.
     */
    public void acquireBytes(long bytes) {
        bytesInProgress.addAndGet(bytes);
    }

    /**
     * Releases bytes previously accounted by {@link #acquireBytes(long)}.
     */
    public void releaseBytes(long bytes) {
        bytesInProgress.addAndGet(-bytes);
    }

    /** Returns the current total bytes in progress (for monitoring). */
    public long getBytesInProgress() {
        return bytesInProgress.get();
    }

    /** Returns the configured bytes limit (0 = unlimited). */
    public long getMaxBytesLimit() {
        return maxBytesLimit;
    }
}
