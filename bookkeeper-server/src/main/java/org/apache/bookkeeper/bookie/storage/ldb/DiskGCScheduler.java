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
//import com.google.common.annotations.VisibleForTesting;
//import io.netty.util.concurrent.DefaultThreadFactory;
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.FileStore;
//import java.nio.file.Files;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.locks.ReadWriteLock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//import org.apache.bookkeeper.bookie.GarbageCollectorThread;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * DiskGCScheduler coordinates garbage collection tasks across multiple directories
// * on the same physical disk to prevent CPU resource contention.
// *
// * <p>Key features:
// * - Only one GC thread runs per physical disk at a time
// * - Round-robin scheduling ensures all directories get GC'd
// * - Supports force GC and suspension operations
// */
//public class DiskGCScheduler {
//    private static final Logger LOG = LoggerFactory.getLogger(DiskGCScheduler.class);
//
//    // Global registry of schedulers per disk
//    private static final Map<String, DiskGCScheduler> DISK_SCHEDULERS = new ConcurrentHashMap<>();
//
//    // Lock for accessing the scheduler registry
//    private static final ReadWriteLock REGISTRY_LOCK = new ReentrantReadWriteLock();
//
//    // The identifier for this disk (canonical path of the root)
//    private final String diskId;
//
//    // List of GC threads managed by this scheduler
//    private final List<GarbageCollectorThread> gcThreads = new ArrayList<>();
//
//    // Current index for round-robin scheduling
//    private final AtomicInteger currentIndex = new AtomicInteger(0);
//
//    // Executor for running the coordination logic
//    private final ScheduledExecutorService coordinatorExecutor;
//
//    // Scheduled task handle
//    private volatile ScheduledFuture<?> scheduledTask;
//
//    // GC interval in milliseconds
//    private final long gcInterval;
//
//    // Whether this scheduler is running
//    private volatile boolean running = false;
//
//    private DiskGCScheduler(String diskId, long gcInterval) {
//        this.diskId = diskId;
//        this.gcInterval = gcInterval;
//        this.coordinatorExecutor = Executors.newSingleThreadScheduledExecutor(
//            new DefaultThreadFactory("DiskGCScheduler-" + diskId.hashCode()));
//        LOG.info("Created DiskGCScheduler for disk: {}", diskId);
//    }
//
//    /**
//     * Get or create a scheduler for the given directory.
//     * Multiple directories on the same disk will share the same scheduler.
//     */
//    public static DiskGCScheduler getOrCreateScheduler(File directory, long gcInterval) throws IOException {
//        String diskId = getDiskId(directory);
//
//        REGISTRY_LOCK.readLock().lock();
//        try {
//            DiskGCScheduler scheduler = DISK_SCHEDULERS.get(diskId);
//            if (scheduler != null) {
//                return scheduler;
//            }
//        } finally {
//            REGISTRY_LOCK.readLock().unlock();
//        }
//
//        // Need to create a new scheduler
//        REGISTRY_LOCK.writeLock().lock();
//        try {
//            // Double-check pattern
//            DiskGCScheduler scheduler = DISK_SCHEDULERS.get(diskId);
//            if (scheduler == null) {
//                scheduler = new DiskGCScheduler(diskId, gcInterval);
//                DISK_SCHEDULERS.put(diskId, scheduler);
//            }
//            return scheduler;
//        } finally {
//            REGISTRY_LOCK.writeLock().unlock();
//        }
//    }
//
//    /**
//     * Register a GC thread with this scheduler.
//     */
//    public synchronized void registerGCThread(GarbageCollectorThread gcThread) {
//        gcThreads.add(gcThread);
//        LOG.info("Registered GC thread for disk: {}, total threads: {}", diskId, gcThreads.size());
//
//        // Start the coordinator if this is the first thread
//        if (gcThreads.size() == 1) {
//            start();
//        }
//    }
//
//    /**
//     * Unregister a GC thread from this scheduler.
//     */
//    public synchronized void unregisterGCThread(GarbageCollectorThread gcThread) {
//        gcThreads.remove(gcThread);
//        LOG.info("Unregistered GC thread for disk: {}, remaining threads: {}", diskId, gcThreads.size());
//
//        // Stop the coordinator if no more threads
//        if (gcThreads.isEmpty()) {
//            stop();
//        }
//    }
//
//    /**
//     * Start the coordinator.
//     */
//    private void start() {
//        if (running) {
//            return;
//        }
//
//        running = true;
//        scheduledTask = coordinatorExecutor.scheduleWithFixedDelay(
//            this::coordinateGC,
//            gcInterval,
//            gcInterval,
//            TimeUnit.MILLISECONDS
//        );
//        LOG.info("Started DiskGCScheduler for disk: {}", diskId);
//    }
//
//    /**
//     * Stop the coordinator.
//     */
//    private void stop() {
//        running = false;
//        if (scheduledTask != null) {
//            scheduledTask.cancel(false);
//            scheduledTask = null;
//        }
//        LOG.info("Stopped DiskGCScheduler for disk: {}", diskId);
//    }
//
//    /**
//     * Coordinate GC execution using round-robin scheduling.
//     */
//    private void coordinateGC() {
//        if (!running || gcThreads.isEmpty()) {
//            return;
//        }
//
//        try {
//            // Use round-robin to select the next GC thread to run
//            int index = currentIndex.getAndUpdate(i -> (i + 1) % gcThreads.size());
//            GarbageCollectorThread selectedGC = gcThreads.get(index);
//
//            LOG.debug("Triggering GC for thread at index {} on disk: {}", index, diskId);
//
//            // Trigger GC on the selected thread
//            selectedGC.triggerGC();
//        } catch (Exception e) {
//            LOG.error("Error coordinating GC on disk: {}", diskId, e);
//        }
//    }
//
//    /**
//     * Force GC on all threads (used for urgent situations like disk full).
//     */
//    public synchronized void forceGCAll() {
//        LOG.info("Force GC triggered for all threads on disk: {}", diskId);
//        for (GarbageCollectorThread gcThread : gcThreads) {
//            gcThread.enableForceGC();
//        }
//    }
//
//    /**
//     * Force GC with specific parameters on all threads.
//     */
//    public synchronized void forceGCAll(boolean forceMajor, boolean forceMinor,
//                                      double majorCompactionThreshold, double minorCompactionThreshold,
//                                      long majorCompactionMaxTimeMillis, long minorCompactionMaxTimeMillis) {
//        LOG.info("Force GC with parameters triggered for all threads on disk: {}", diskId);
//        for (GarbageCollectorThread gcThread : gcThreads) {
//            gcThread.enableForceGC(forceMajor, forceMinor, majorCompactionThreshold,
//                                 minorCompactionThreshold, majorCompactionMaxTimeMillis,
//                                 minorCompactionMaxTimeMillis);
//        }
//    }
//
//    /**
//     * Suspend major GC on all threads.
//     */
//    public synchronized void suspendMajorGCAll() {
//        LOG.info("Suspending major GC for all threads on disk: {}", diskId);
//        for (GarbageCollectorThread gcThread : gcThreads) {
//            gcThread.suspendMajorGC();
//        }
//    }
//
//    /**
//     * Resume major GC on all threads.
//     */
//    public synchronized void resumeMajorGCAll() {
//        LOG.info("Resuming major GC for all threads on disk: {}", diskId);
//        for (GarbageCollectorThread gcThread : gcThreads) {
//            gcThread.resumeMajorGC();
//        }
//    }
//
//    /**
//     * Suspend minor GC on all threads.
//     */
//    public synchronized void suspendMinorGCAll() {
//        LOG.info("Suspending minor GC for all threads on disk: {}", diskId);
//        for (GarbageCollectorThread gcThread : gcThreads) {
//            gcThread.suspendMinorGC();
//        }
//    }
//
//    /**
//     * Resume minor GC on all threads.
//     */
//    public synchronized void resumeMinorGCAll() {
//        LOG.info("Resuming minor GC for all threads on disk: {}", diskId);
//        for (GarbageCollectorThread gcThread : gcThreads) {
//            gcThread.resumeMinorGC();
//        }
//    }
//
//    /**
//     * Shutdown this scheduler and cleanup resources.
//     */
//    public synchronized void shutdown() throws InterruptedException {
//        stop();
//        coordinatorExecutor.shutdown();
//        coordinatorExecutor.awaitTermination(5, TimeUnit.SECONDS);
//
//        // Remove from global registry
//        REGISTRY_LOCK.writeLock().lock();
//        try {
//            DISK_SCHEDULERS.remove(diskId);
//        } finally {
//            REGISTRY_LOCK.writeLock().unlock();
//        }
//
//        LOG.info("Shutdown DiskGCScheduler for disk: {}", diskId);
//    }
//
//    /**
//     * Get the disk identifier for a directory.
//     * Uses the FileStore to identify the underlying physical disk.
//     */
//    @VisibleForTesting
//    static String getDiskId(File directory) throws IOException {
//        try {
//            FileStore fileStore = Files.getFileStore(directory.toPath());
//            // Use a combination of name and type to create a unique identifier
//            // This helps distinguish different disks even if they have similar names
//            return fileStore.name() + ":" + fileStore.type();
//        } catch (IOException e) {
//            LOG.warn("Unable to determine FileStore for directory: {}, falling back to canonical path",
//                     directory.getAbsolutePath(), e);
//            // Fallback to canonical path approach
//            return directory.getCanonicalFile().toPath().getRoot().toString();
//        }
//    }
//
//    @VisibleForTesting
//    synchronized List<GarbageCollectorThread> getRegisteredThreads() {
//        return new ArrayList<>(gcThreads);
//    }
//
//    @VisibleForTesting
//    boolean isRunning() {
//        return running;
//    }
//
//    @VisibleForTesting
//    static void clearSchedulers() {
//        REGISTRY_LOCK.writeLock().lock();
//        try {
//            DISK_SCHEDULERS.clear();
//        } finally {
//            REGISTRY_LOCK.writeLock().unlock();
//        }
//    }
//}