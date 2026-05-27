/*
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
 */
package org.apache.bookkeeper.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MemoryLimitController}.
 */
public class MemoryLimitControllerTest {

    private static final String GAUGE_KEY = "test_bytes_in_progress";

    private ExecutorService executor;

    @Before
    public void setup() {
        executor = Executors.newCachedThreadPool();
    }

    @After
    public void teardown() {
        executor.shutdownNow();
    }

    private MemoryLimitController newController(long limit) {
        return new MemoryLimitController(limit, GAUGE_KEY, NullStatsLogger.INSTANCE);
    }

    // -----------------------------------------------------------------------
    // tryAcquireBytes
    // -----------------------------------------------------------------------

    @Test
    public void testTryAcquireUnderLimit() {
        MemoryLimitController controller = newController(100);

        assertTrue(controller.tryAcquireBytes(50));
        assertEquals(50, controller.getBytesInProgress());

        assertTrue(controller.tryAcquireBytes(50));
        assertEquals(100, controller.getBytesInProgress());
    }

    @Test
    public void testTryAcquireExactlyAtLimit() {
        MemoryLimitController controller = newController(100);

        // Acquire exactly the limit — should succeed
        assertTrue(controller.tryAcquireBytes(100));
        assertEquals(100, controller.getBytesInProgress());
    }

    @Test
    public void testTryAcquireOverLimitRejected() {
        MemoryLimitController controller = newController(100);

        assertTrue(controller.tryAcquireBytes(100));
        // One more byte over the limit — should be rejected
        assertFalse(controller.tryAcquireBytes(1));
        // bytesInProgress must stay unchanged on rejection
        assertEquals(100, controller.getBytesInProgress());
    }

    @Test
    public void testTryAcquireAfterRelease() {
        MemoryLimitController controller = newController(100);

        assertTrue(controller.tryAcquireBytes(100));
        assertFalse(controller.tryAcquireBytes(1));

        controller.releaseBytes(50);
        assertEquals(50, controller.getBytesInProgress());

        // Should succeed now that we have room
        assertTrue(controller.tryAcquireBytes(50));
        assertEquals(100, controller.getBytesInProgress());
    }

    @Test
    public void testTryAcquireSingleByteAtBoundary() {
        MemoryLimitController controller = newController(1);

        assertTrue(controller.tryAcquireBytes(1));
        assertFalse(controller.tryAcquireBytes(1));

        controller.releaseBytes(1);
        assertTrue(controller.tryAcquireBytes(1));
    }

    // -----------------------------------------------------------------------
    // acquireBytes / releaseBytes
    // -----------------------------------------------------------------------

    @Test
    public void testAcquireAndRelease() {
        MemoryLimitController controller = newController(100);

        controller.acquireBytes(60);
        assertEquals(60, controller.getBytesInProgress());

        controller.releaseBytes(60);
        assertEquals(0, controller.getBytesInProgress());
    }

    @Test
    public void testMultipleAcquireAndRelease() {
        MemoryLimitController controller = newController(1000);

        controller.acquireBytes(100);
        controller.acquireBytes(200);
        controller.acquireBytes(300);
        assertEquals(600, controller.getBytesInProgress());

        controller.releaseBytes(100);
        assertEquals(500, controller.getBytesInProgress());

        controller.releaseBytes(500);
        assertEquals(0, controller.getBytesInProgress());
    }

    // -----------------------------------------------------------------------
    // isOverLimit
    // -----------------------------------------------------------------------

    @Test
    public void testIsOverLimitFalseWhenUnder() {
        MemoryLimitController controller = newController(100);
        controller.acquireBytes(50);
        assertFalse(controller.isOverLimit());
    }

    @Test
    public void testIsOverLimitTrueWhenAtLimit() {
        MemoryLimitController controller = newController(100);
        controller.acquireBytes(100);
        assertTrue(controller.isOverLimit());
    }

    @Test
    public void testIsOverLimitTrueWhenOver() {
        MemoryLimitController controller = newController(100);
        // acquireBytes does not enforce the limit
        controller.acquireBytes(150);
        assertTrue(controller.isOverLimit());
    }

    // -----------------------------------------------------------------------
    // getMaxBytesLimit / getBytesInProgress
    // -----------------------------------------------------------------------

    @Test
    public void testGetMaxBytesLimit() {
        MemoryLimitController controller = newController(512);
        assertEquals(512, controller.getMaxBytesLimit());
    }

    @Test
    public void testGetBytesInProgressInitiallyZero() {
        MemoryLimitController controller = newController(100);
        assertEquals(0, controller.getBytesInProgress());
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    @Test
    public void testConcurrentTryAcquireRespectLimit() throws Exception {
        final long limit = 1000;
        MemoryLimitController controller = newController(limit);
        int threads = 20;
        int acquirePerThread = 10;
        long bytesPerAcquire = 10; // total attempted = 20 * 10 * 10 = 2000 > 1000

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int j = 0; j < acquirePerThread; j++) {
                    if (controller.tryAcquireBytes(bytesPerAcquire)) {
                        successCount.incrementAndGet();
                    }
                }
            }));
        }

        start.countDown();
        for (Future<?> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }

        // Only up to limit / bytesPerAcquire slots can be acquired
        long maxSlots = limit / bytesPerAcquire;
        assertTrue("successCount should not exceed maxSlots",
                successCount.get() <= maxSlots);
        // bytesInProgress must equal successCount * bytesPerAcquire
        assertEquals(successCount.get() * bytesPerAcquire, controller.getBytesInProgress());
    }

    @Test
    public void testConcurrentAcquireAndRelease() throws Exception {
        MemoryLimitController controller = newController(10000);
        int threads = 10;
        int opsPerThread = 100;
        long bytes = 5;

        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int j = 0; j < opsPerThread; j++) {
                    controller.acquireBytes(bytes);
                    controller.releaseBytes(bytes);
                }
            }));
        }

        start.countDown();
        for (Future<?> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }

        // All acquires and releases are balanced — final value must be 0
        assertEquals(0, controller.getBytesInProgress());
    }
}
