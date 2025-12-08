///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.bookkeeper.bookie.storage.ldb;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertSame;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.ArgumentMatchers.anyBoolean;
//import static org.mockito.ArgumentMatchers.anyDouble;
//import static org.mockito.ArgumentMatchers.anyLong;
//import static org.mockito.Mockito.atLeast;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import org.apache.bookkeeper.bookie.GarbageCollectorThread;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//
///**
// * Unit tests for DiskGCScheduler.
// */
//public class DiskGCSchedulerTest {
//
//    @Rule
//    public TemporaryFolder tempDir = new TemporaryFolder();
//
//    private File testDir1;
//    private File testDir2;
//    private File testDir3; // Different disk
//    private GarbageCollectorThread gcThread1;
//    private GarbageCollectorThread gcThread2;
//    private GarbageCollectorThread gcThread3;
//
//    @Before
//    public void setUp() throws IOException {
//        // Clear any existing schedulers from previous tests
//        DiskGCScheduler.clearSchedulers();
//
//        testDir1 = tempDir.newFolder("dir1");
//        testDir2 = tempDir.newFolder("dir2");
//        testDir3 = tempDir.newFolder("dir3");
//
//        gcThread1 = mock(GarbageCollectorThread.class);
//        gcThread2 = mock(GarbageCollectorThread.class);
//        gcThread3 = mock(GarbageCollectorThread.class);
//    }
//
//    @After
//    public void tearDown() {
//        DiskGCScheduler.clearSchedulers();
//    }
//
//    @Test
//    public void testSameDiskSharesScheduler() throws IOException {
//        // Directories on the same disk should share the same scheduler
//        DiskGCScheduler scheduler1 = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//        DiskGCScheduler scheduler2 = DiskGCScheduler.getOrCreateScheduler(testDir2, 1000);
//
//        // Should be the same instance since they're on the same disk
//        assertSame("Directories on same disk should share scheduler", scheduler1, scheduler2);
//    }
//
//    @Test
//    public void testRegisterUnregisterGCThreads() throws IOException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//
//        // Initially no threads registered
//        assertEquals(0, scheduler.getRegisteredThreads().size());
//        assertFalse("Scheduler should not be running initially", scheduler.isRunning());
//
//        // Register first thread - should start scheduler
//        scheduler.registerGCThread(gcThread1);
//        assertEquals(1, scheduler.getRegisteredThreads().size());
//        assertTrue("Scheduler should be running after first registration", scheduler.isRunning());
//
//        // Register second thread
//        scheduler.registerGCThread(gcThread2);
//        assertEquals(2, scheduler.getRegisteredThreads().size());
//        assertTrue("Scheduler should still be running", scheduler.isRunning());
//
//        // Unregister first thread
//        scheduler.unregisterGCThread(gcThread1);
//        assertEquals(1, scheduler.getRegisteredThreads().size());
//        assertTrue("Scheduler should still be running with one thread", scheduler.isRunning());
//
//        // Unregister last thread - should stop scheduler
//        scheduler.unregisterGCThread(gcThread2);
//        assertEquals(0, scheduler.getRegisteredThreads().size());
//        assertFalse("Scheduler should stop when no threads registered", scheduler.isRunning());
//    }
//
//    @Test
//    public void testRoundRobinScheduling() throws IOException, InterruptedException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 100); // Short interval for testing
//
//        // Register multiple threads
//        scheduler.registerGCThread(gcThread1);
//        scheduler.registerGCThread(gcThread2);
//
//        // Wait for a few scheduling cycles
//        Thread.sleep(500);
//
//        // Both threads should have been triggered at least once
//        verify(gcThread1, atLeast(1)).triggerGC();
//        verify(gcThread2, atLeast(1)).triggerGC();
//    }
//
//    @Test
//    public void testForceGCAll() throws IOException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//
//        scheduler.registerGCThread(gcThread1);
//        scheduler.registerGCThread(gcThread2);
//
//        // Trigger force GC on all threads
//        scheduler.forceGCAll();
//
//        verify(gcThread1, times(1)).enableForceGC();
//        verify(gcThread2, times(1)).enableForceGC();
//    }
//
//    @Test
//    public void testForceGCAllWithParameters() throws IOException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//
//        scheduler.registerGCThread(gcThread1);
//        scheduler.registerGCThread(gcThread2);
//
//        // Trigger force GC with parameters
//        scheduler.forceGCAll(true, false, 0.7, 0.3, 10000, 5000);
//
//        verify(gcThread1, times(1)).enableForceGC(true, false, 0.7, 0.3, 10000, 5000);
//        verify(gcThread2, times(1)).enableForceGC(true, false, 0.7, 0.3, 10000, 5000);
//    }
//
//    @Test
//    public void testSuspendResumeOperations() throws IOException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//
//        scheduler.registerGCThread(gcThread1);
//        scheduler.registerGCThread(gcThread2);
//
//        // Test suspend major GC
//        scheduler.suspendMajorGCAll();
//        verify(gcThread1, times(1)).suspendMajorGC();
//        verify(gcThread2, times(1)).suspendMajorGC();
//
//        // Test resume major GC
//        scheduler.resumeMajorGCAll();
//        verify(gcThread1, times(1)).resumeMajorGC();
//        verify(gcThread2, times(1)).resumeMajorGC();
//
//        // Test suspend minor GC
//        scheduler.suspendMinorGCAll();
//        verify(gcThread1, times(1)).suspendMinorGC();
//        verify(gcThread2, times(1)).suspendMinorGC();
//
//        // Test resume minor GC
//        scheduler.resumeMinorGCAll();
//        verify(gcThread1, times(1)).resumeMinorGC();
//        verify(gcThread2, times(1)).resumeMinorGC();
//    }
//
//    @Test
//    public void testShutdown() throws IOException, InterruptedException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//
//        scheduler.registerGCThread(gcThread1);
//        assertTrue("Scheduler should be running", scheduler.isRunning());
//
//        scheduler.shutdown();
//        assertFalse("Scheduler should be stopped after shutdown", scheduler.isRunning());
//
//        // Verify scheduler is removed from global registry
//        DiskGCScheduler newScheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//        assertNotNull("Should be able to create new scheduler after shutdown", newScheduler);
//    }
//
//    @Test
//    public void testGetDiskId() throws IOException {
//        String diskId1 = DiskGCScheduler.getDiskId(testDir1);
//        String diskId2 = DiskGCScheduler.getDiskId(testDir2);
//
//        assertNotNull("Disk ID should not be null", diskId1);
//        assertNotNull("Disk ID should not be null", diskId2);
//
//        // Directories in the same temp folder should have the same disk ID
//        assertEquals("Directories on same disk should have same disk ID", diskId1, diskId2);
//    }
//
//    @Test
//    public void testNoSchedulingWhenNoThreads() throws IOException, InterruptedException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 100);
//
//        // Don't register any threads
//        Thread.sleep(300);
//
//        // No GC should be triggered since no threads are registered
//        verify(gcThread1, never()).triggerGC();
//        verify(gcThread2, never()).triggerGC();
//        assertFalse("Scheduler should not be running with no threads", scheduler.isRunning());
//    }
//
//    @Test
//    public void testConcurrentRegistration() throws IOException, InterruptedException {
//        DiskGCScheduler scheduler = DiskGCScheduler.getOrCreateScheduler(testDir1, 1000);
//
//        CountDownLatch latch = new CountDownLatch(2);
//
//        // Simulate concurrent registration from different threads
//        Thread t1 = new Thread(() -> {
//            scheduler.registerGCThread(gcThread1);
//            latch.countDown();
//        });
//
//        Thread t2 = new Thread(() -> {
//            scheduler.registerGCThread(gcThread2);
//            latch.countDown();
//        });
//
//        t1.start();
//        t2.start();
//
//        assertTrue("Threads should complete registration", latch.await(5, TimeUnit.SECONDS));
//        assertEquals("Both threads should be registered", 2, scheduler.getRegisteredThreads().size());
//        assertTrue("Scheduler should be running", scheduler.isRunning());
//    }
//}