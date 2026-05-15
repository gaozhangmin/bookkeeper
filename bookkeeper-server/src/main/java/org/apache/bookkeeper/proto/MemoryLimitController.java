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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_BYTES_IN_PROGRESS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_BYTES_IN_PROGRESS;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Controls memory back-pressure for in-flight write and read requests.
 *
 * <p>Tracks the total bytes currently occupied by:
 * <ul>
 *   <li>Write requests: from the moment an add-entry request is accepted until the bookie
 *       finishes writing and the {@link WriteEntryProcessor} is recycled.</li>
 *   <li>Read requests: from the moment storage returns data until the
 *       {@link ReadEntryProcessor} is recycled (i.e., the response has been sent).</li>
 * </ul>
 *
 * <p>When the tracked bytes exceed the configured limit, new requests are rejected
 * immediately with {@code ETOOMANYREQUESTS}. A limit of {@code 0} disables the check.
 */
public class MemoryLimitController {

    /** Total bytes of add requests currently in-flight. */
    private final AtomicLong writeBytesInProgress = new AtomicLong(0);

    /** Maximum total bytes allowed for in-flight add requests. 0 means unlimited. */
    private final long maxWriteBytesLimit;

    /** Total bytes of read responses currently in-flight. */
    private final AtomicLong readBytesInProgress = new AtomicLong(0);

    /** Maximum total bytes allowed for in-flight read responses. 0 means unlimited. */
    private final long maxReadBytesLimit;

    public MemoryLimitController(long maxWriteBytesLimit, long maxReadBytesLimit, StatsLogger statsLogger) {
        this.maxWriteBytesLimit = maxWriteBytesLimit;
        this.maxReadBytesLimit = maxReadBytesLimit;
        registerGauges(statsLogger);
    }

    private void registerGauges(StatsLogger statsLogger) {
        statsLogger.registerGauge(WRITE_BYTES_IN_PROGRESS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return writeBytesInProgress.get();
            }
        });

        statsLogger.registerGauge(READ_BYTES_IN_PROGRESS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readBytesInProgress.get();
            }
        });
    }

    // -------------------------------------------------------------------------
    // Write memory
    // -------------------------------------------------------------------------

    /**
     * Attempts to account {@code bytes} for a new write request.
     *
     * <p>If the limit is disabled (0) or the new total would not exceed the limit,
     * the bytes are atomically added to {@link #writeBytesInProgress} and {@code false} is returned
     * (meaning the request is accepted and bytes are accounted).
     *
     * <p>If the new total would exceed the limit, nothing is changed and {@code true} is returned
     * (meaning the request should be rejected).
     *
     * @param bytes the size of the new request
     * @return {@code true} if the request should be rejected; {@code false} if accepted and accounted
     */
    public boolean tryAcquireWriteBytes(long bytes) {
        if (maxWriteBytesLimit <= 0) {
            return false; // unlimited, not accounted either
        }
        long newTotal = writeBytesInProgress.addAndGet(bytes);
        if (newTotal > maxWriteBytesLimit) {
            writeBytesInProgress.addAndGet(-bytes);
            return true; // over limit → reject
        }
        return false; // accepted and accounted
    }

    /**
     * Accounts {@code bytes} for an accepted write request.
     * Must be paired with a later call to {@link #releaseWriteBytes(long)}.
     */
    public void acquireWriteBytes(long bytes) {
        writeBytesInProgress.addAndGet(bytes);
    }

    /**
     * Releases bytes previously accounted by {@link #acquireWriteBytes(long)}.
     */
    public void releaseWriteBytes(long bytes) {
        writeBytesInProgress.addAndGet(-bytes);
    }

    /** Returns the current total write bytes in progress (for monitoring). */
    public long getWriteBytesInProgress() {
        return writeBytesInProgress.get();
    }

    /** Returns the configured write bytes limit (0 = unlimited). */
    public long getMaxWriteBytesLimit() {
        return maxWriteBytesLimit;
    }

    // -------------------------------------------------------------------------
    // Read memory
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if the read memory limit is enabled and the current
     * in-progress bytes already meet or exceed the limit.
     *
     * <p>Unlike write bytes, the actual size of a read response is not known at request
     * arrival time, so we only guard against accepting new requests when the current total
     * is already at the limit.
     *
     * @return {@code true} if the request should be rejected
     */
    public boolean isReadMemoryLimitExceeded() {
        return maxReadBytesLimit > 0 && readBytesInProgress.get() >= maxReadBytesLimit;
    }

    /**
     * Accounts {@code bytes} for a read response that has been loaded into memory.
     * Must be paired with a later call to {@link #releaseReadBytes(long)}.
     */
    public void acquireReadBytes(long bytes) {
        readBytesInProgress.addAndGet(bytes);
    }

    /**
     * Releases bytes previously accounted by {@link #acquireReadBytes(long)}.
     */
    public void releaseReadBytes(long bytes) {
        readBytesInProgress.addAndGet(-bytes);
    }

    /** Returns the current total read bytes in progress (for monitoring). */
    public long getReadBytesInProgress() {
        return readBytesInProgress.get();
    }

    /** Returns the configured read bytes limit (0 = unlimited). */
    public long getMaxReadBytesLimit() {
        return maxReadBytesLimit;
    }
}
