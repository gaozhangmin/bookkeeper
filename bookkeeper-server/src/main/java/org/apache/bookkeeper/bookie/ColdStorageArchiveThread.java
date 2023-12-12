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

package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.util.BookKeeperConstants.METADATA_CACHE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

import org.apache.bookkeeper.bookie.BookieException.EntryLogMetadataMapException;
import org.apache.bookkeeper.bookie.stats.ColdStorageArchiveStats;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.ldb.DirectDbSingleLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.EntryLocationIndex;
import org.apache.bookkeeper.bookie.storage.ldb.PersistentEntryLogMetadataMap;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class ColdStorageArchiveThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ColdStorageArchiveThread.class);
    private static final String LOG_FILE_SUFFIX = ".log";

    private final CopyOnWriteArrayList<ArchiverThreadListener>
            listeners = new CopyOnWriteArrayList<>();

    private final long archiveIntervalMs;

    private final File coldLedgerDir;

    private final File ledgerDir;

    private Throttler throttler = null;
    EntryLogger coldEntryLogger;

    // Maps entry log files to the set of ledgers that comprise the file and the size usage per ledger
    EntryLogMetadataMap entryLogMetaMap;

    final ScheduledExecutorService gcExecutor;
    Future<?> scheduledFuture = null;

    // Entry Logger Handle
    final EntryLogger entryLogger;
    // Stats loggers for garbage collection operations
    public final ColdStorageArchiveStats stats;

    private volatile long totalEntryLogSize;
    private volatile int numActiveEntryLogs;

    final DirectDbSingleLedgerStorage ledgerStorage;
    final ServerConfiguration conf;
    final LedgerDirsManager ledgerDirsManager;

    final AbstractLogCompactor.Throttler scanThrottler;

    final AtomicBoolean forceArchive = new AtomicBoolean(false);
    private final ByteBufAllocator allocator;
    private final AtomicLong archivedMaxLogId = new AtomicLong(-1L);
    final ArchiveScannerFactory scannerFactory = new ArchiveScannerFactory();
    protected volatile WriteCache writeCache;
    protected volatile WriteCache writeCacheForDowngrade;
    private final EntryLocationIndex entryLocationIndex;
    protected final ReentrantLock flushMutex = new ReentrantLock();
    private final StampedLock writeCacheRotationLock = new StampedLock();

    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public ColdStorageArchiveThread(ServerConfiguration conf,
                                    final LedgerDirsManager ledgerDirsManager,
                                    final LedgerDirsManager coldLedgerDirsManager,
                                    final DirectDbSingleLedgerStorage ledgerStorage,
                                    EntryLogger entryLogger,
                                    EntryLogger coldEntryLogger, StatsLogger statsLogger,
                                    ByteBufAllocator allocator,
                                    WriteCache writeCache,
                                    EntryLocationIndex entryLocationIndex) throws IOException {
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("ColdStorageArchiveThread"));
        this.writeCache = writeCache;
        this.entryLocationIndex = entryLocationIndex;
        this.conf = conf;
        this.allocator = allocator;
        this.ledgerDirsManager = ledgerDirsManager;
        this.entryLogger = entryLogger;
        this.entryLogMetaMap = createEntryLogMetadataMap();
        this.ledgerStorage = ledgerStorage;
        this.numActiveEntryLogs = 0;
        this.totalEntryLogSize = 0L;
        this.stats = new ColdStorageArchiveStats(
                statsLogger,
                () -> numActiveEntryLogs,
                () -> totalEntryLogSize
        );
        this.scanThrottler = new AbstractLogCompactor.Throttler(conf);

        this.archiveIntervalMs = conf.getColdStorageArchiveInterval();
        this.coldLedgerDir = coldLedgerDirsManager.getAllLedgerDirs().get(0);
        this.ledgerDir = ledgerDirsManager.getAllLedgerDirs().get(0);
        this.coldEntryLogger = coldEntryLogger;
        int archiveRateByBytes = conf.getArchiveRateBytes();
        if (archiveRateByBytes > 0) {
            this.throttler = new Throttler(archiveRateByBytes);
        }
    }

    public void setWriteCacheForDowngrade() {
        this.writeCacheForDowngrade = new WriteCache(allocator,
                conf.getEntryLogSizeLimit() / conf.getColdLedgerDirs().length);
    }

    public void destoryWriteCacheForDowngrade() throws IOException {
        triggerFlush();
        this.writeCacheForDowngrade.close();
        this.writeCacheForDowngrade = null;
    }

    private EntryLogMetadataMap createEntryLogMetadataMap() throws IOException {
        if (conf.isGcEntryLogMetadataCacheEnabled()) {
            String baseDir = Strings.isNullOrEmpty(conf.getGcEntryLogMetadataCachePath())
                    ? this.ledgerDirsManager.getAllLedgerDirs().get(0).getPath() : conf.getGcEntryLogMetadataCachePath();
            try {
                return new PersistentEntryLogMetadataMap(baseDir, conf);
            } catch (IOException e) {
                LOG.error("Failed to initialize persistent-metadata-map , clean up {}",
                        baseDir + "/" + METADATA_CACHE, e);
                throw e;
            }
        } else {
            return new InMemoryEntryLogMetadataMap();
        }
    }


    public void enableForceArchive() {
        if (forceArchive.compareAndSet(false, true)) {
            LOG.info("Forced coldStorage archive triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void disableForceArchive() {
        if (forceArchive.compareAndSet(true, false)) {
            LOG.info("{} disabled force coldStorage Archive since bookie has enough space now.", Thread
                    .currentThread().getName());
        }
    }

    private LedgerDirsManager.LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsManager.LedgerDirsListener() {

            @Override
            public void diskAlmostFull(File disk) {
                if (disk.getPath().equals(ledgerDir.getPath())) {
                    // we are running out of space, enable force archive
                    enableForceArchive();
                }
            }

            @Override
            public void diskWritable(File disk) {
                if (disk.getPath().equals(ledgerDir.getPath())) {
                    // we have enough space now
                    disableForceArchive();
                }
            }
        };
    }


    public void start() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        scheduledFuture = gcExecutor.scheduleAtFixedRate(this, 0,
                archiveIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        runWithFlags();
    }

    public void runWithFlags() {
        long threadStart = MathUtils.nowInNano();

        try {
            // Extract all the ledger ID's that comprise all the entry logs
            // (except for the current new one which is still being written to).
            if (conf.getDiskCacheRetentionTime() < 0) {
                return;
            }
            extractMetaFromEntryLogs();
            stats.getExtractionRunTime().registerSuccessfulEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);

            archiveToColdStorage();

            stats.getArchiveThreadRuntime().registerSuccessfulEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);
        } catch (EntryLogMetadataMapException e) {
            LOG.error(" Failed to archive, error in entryLog-metadatamap", e);
            stats.getArchiveThreadRuntime().registerFailedEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);
        } catch (IOException ioe) {
            LOG.error("Failed to update last archived log id", ioe);
        }
    }

    /**
     * Remove entry log after archiving.
     *
     * @param entryLogId
     *          Entry Log File Id
     * @throws EntryLogMetadataMapException
     */
    protected void removeEntryLogByArchive(long entryLogId) throws EntryLogMetadataMapException {
        for (ArchiverThreadListener listener : listeners) {
            listener.onArchiveEntryLog(entryLogId);
        }
        if (entryLogger.removeEntryLog(entryLogId)) {
            LOG.info("Removing EntryLog={}-{} After archiving", entryLogId, Long.toHexString(entryLogId));
            entryLogMetaMap.remove(entryLogId);
        }
    }

    protected void removeEmptyEntryLog(long entryLogId) throws EntryLogMetadataMapException {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            LOG.info("Removing empty EntryLog={}-{} during archiving", entryLogId, Long.toHexString(entryLogId));
            entryLogMetaMap.remove(entryLogId);
        }
    }

    /**
     * Compact entry logs if necessary.
     *
     * <p>
     * Compaction will be executed from low unused space to high unused space.
     * Those entry log files whose remaining size percentage is higher than threshold
     * would not be compacted.
     * </p>
     */
    @VisibleForTesting
    void archiveToColdStorage() throws EntryLogMetadataMapException, IOException {
        long currentTime = System.currentTimeMillis();
        if (this.throttler != null) {
            this.throttler.resetRate(this.conf.getArchiveRateBytes());
        }
        AtomicLong totalEntryLogSizeAcc = new AtomicLong(0L);
        this.entryLogMetaMap.forEach((entryLogId, meta) -> {
            if (meta == null) {
                LOG.info("ArchiveToColdStorage: Metadata for entry log {} already deleted", entryLogId);
                return;
            }
            if (!forceArchive.get()
                    && currentTime - meta.getFlushTimestamp() <= conf.getDiskCacheRetentionTime()) {
                totalEntryLogSizeAcc.getAndAdd(meta.getRemainingSize());
                return;
            }
            try {

                entryLogger.scanEntryLog(meta.getEntryLogId(), scannerFactory.newScanner(meta));
                triggerFlush();

                this.removeEntryLogByArchive(entryLogId);
                stats.getReclaimedDiskCacheSpaceViaArchive().addCount(meta.getTotalSize());

                if (entryLogId > archivedMaxLogId.get()) {
                    archivedMaxLogId.set(entryLogId);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("LedgerDir={}, coldLedgerDir={}, Archived entryLog={}-{}",
                            ledgerDir, coldLedgerDir, entryLogId, Long.toHexString(entryLogId));
                }
            } catch (IOException ex) {
                LOG.warn("EntryLog={}-{} is not found during archiving process",
                        entryLogId, Long.toHexString(entryLogId), ex);
            } catch (EntryLogMetadataMapException e) {
                LOG.error("Error in entryLog-metadatamap, Failed to archive entryLog={}-{}",
                        entryLogId, Long.toHexString(entryLogId), e);
            } catch (Exception unexpected) {
                LOG.error("Unexpected exception while archiving entryLog={}-{}",
                        entryLogId, Long.toHexString(entryLogId), unexpected);
            }
        });
        LOG.info("LedgerDir={}, coldLedgerDir={}, archiveToColdStorage finished, archivedMaxLogId={}-{}",
                ledgerDir, coldLedgerDir, archivedMaxLogId.get(), Long.toHexString(archivedMaxLogId.get()));
        this.numActiveEntryLogs = entryLogMetaMap.size();
        this.totalEntryLogSize = totalEntryLogSizeAcc.get();
    }

    /**
     * Method to read in all the entry logs (those that we haven't done so yet),
     * and find the set of ledger ID's that make up each entry log file.
     *
     * @throws EntryLogMetadataMapException
     */
    protected void extractMetaFromEntryLogs() throws EntryLogMetadataMapException {

        for (long entryLogId : entryLogger.getFlushedLogIds()) {
            // Comb the current entry log file if it has not already been extracted.
            if (entryLogMetaMap.containsKey(entryLogId)) {
                continue;
            }

            // check whether log file exists or not
            // if it doesn't exist, this log file might have been garbage collected.
            if (!entryLogger.logExists(entryLogId)) {
                continue;
            }

            LOG.info("Extracting entry log meta from entryLogId: {}", entryLogId);

            try {
                // Read through the entry log file and extract the entry log meta
                EntryLogMetadata entryLogMeta = entryLogger.getEntryLogMetadata(entryLogId, scanThrottler);
                removeIfLedgerNotExists(entryLogMeta);
                if (entryLogMeta.isEmpty()) {
                    // This means the entry log is not associated with any active
                    // ledgers anymore.
                    // We can remove this entry log file now.
                    this.removeEmptyEntryLog(entryLogId);
                } else {
                    if (entryLogMeta.getFlushTimestamp() == -1L) {
//                        BasicFileAttributes attributes = Files.readAttributes(
//                                Path.of(ledgerDir.getPath(), Long.toHexString(entryLogId) + LOG_FILE_SUFFIX),
//                                BasicFileAttributes.class);
//                        long creationTime = attributes.lastModifiedTime().toMillis();
//                        entryLogMeta.setFlushTimestamp(creationTime);
                    }
                    entryLogMetaMap.put(entryLogId, entryLogMeta);
                }
            } catch (IOException e) {
                LOG.warn("Premature exception when processing " + entryLogId
                        + " recovery will take care of the problem", e);
            }
        }
    }

    public void removeIfLedgerNotExists(EntryLogMetadata meta) {
        MutableBoolean modified = new MutableBoolean(false);
        meta.removeLedgerIf((entryLogLedger) -> {
            // Remove the entry log ledger from the set if it isn't active.
            try {
                boolean exist = ledgerStorage.ledgerExists(entryLogLedger);
                if (!exist) {
                    modified.setTrue();
                }
                return !exist;
            } catch (IOException e) {
                LOG.error("Error reading from ledger storage", e);
                return false;
            }
        });

        modified.getValue();
    }

    public void addEntry(ByteBuf entry) throws IOException {
        long startTime = MathUtils.nowInNano();
        long ledgerId = entry.getLong(entry.readerIndex());
        long entryId = entry.getLong(entry.readerIndex() + 8);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Add archived entry. {}@{}", ledgerId, entryId);
        }
        if (writeCacheForDowngrade != null) {
            boolean inserted = writeCacheForDowngrade.put(ledgerId, entryId, entry);
            if (!inserted) {
                triggerFlush();
                writeCacheForDowngrade.put(ledgerId, entryId, entry);
            }
        } else {
            long stamp = this.ledgerStorage.getWriteCacheRotationLock().tryOptimisticRead();
            boolean inserted = writeCache.put(ledgerId, entryId, entry);
            if (!this.ledgerStorage.getWriteCacheRotationLock().validate(stamp)) {
                // The write cache was rotated while we were inserting. We need to acquire the proper read lock and repeat
                // the operation because we might have inserted in a write cache that was already being flushed and cleared,
                // without being sure about this last entry being flushed or not.
                stamp = this.ledgerStorage.getWriteCacheRotationLock().readLock();
                try {
                    inserted = writeCache.put(ledgerId, entryId, entry);
                } finally {
                    this.ledgerStorage.getWriteCacheRotationLock().unlockRead(stamp);
                }
            }
            if (!inserted) {
                triggerFlush();
                stamp = this.ledgerStorage.getWriteCacheRotationLock().readLock();
                try {
                    writeCache.put(ledgerId, entryId, entry);
                } finally {
                    this.ledgerStorage.getWriteCacheRotationLock().unlockRead(stamp);
                }
            }
        }
        stats.getArchiveEntryStats().registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
    }

    public void triggerFlush() throws IOException {
        flushMutex.lock();
        long startTime = MathUtils.nowInNano();
        try {
            List<EntryLocation> offsets = new ArrayList<>();
            long sizeToFlush;
            if (writeCacheForDowngrade != null) {
                sizeToFlush = writeCacheForDowngrade.size();
                writeCacheForDowngrade.forEach((lId, eId, cacheEntry) -> {
                    long newoffset = coldEntryLogger.addEntry(lId, cacheEntry);
                    offsets.add(new EntryLocation(lId, eId, newoffset));
                });
                writeCacheForDowngrade.clear();
            } else {
                this.ledgerStorage.getFlushMutex().lock();
                sizeToFlush = writeCache.size();
                writeCache.forEach((lId, eId, cacheEntry) -> {
                    long newoffset = coldEntryLogger.addEntry(lId, cacheEntry);
                    offsets.add(new EntryLocation(lId, eId, newoffset));
                });
                writeCache.clear();
            }
            long entryLoggerStart = MathUtils.nowInNano();
            coldEntryLogger.flush();
            stats.getFlushEntryLogStats().registerSuccessfulEvent(MathUtils.elapsedNanos(entryLoggerStart), TimeUnit.NANOSECONDS);

            long batchFlushStartTime = MathUtils.nowInNano();
            entryLocationIndex.updateLocations(offsets);
            stats.getFlushEntryLogStats().registerSuccessfulEvent(MathUtils.elapsedNanos(batchFlushStartTime), TimeUnit.NANOSECONDS);


            double flushTimeSeconds = MathUtils.elapsedNanos(startTime) / (double) TimeUnit.SECONDS.toNanos(1);
            double flushThroughput = sizeToFlush  / flushTimeSeconds;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Flushing done time {} s -- Written {} MB/s", flushTimeSeconds, flushThroughput);
            }
            stats.getFlushStats().registerSuccessfulEvent(MathUtils.elapsedNanos(startTime),
                    TimeUnit.NANOSECONDS);

            stats.getFlushSizeStats().registerSuccessfulValue(sizeToFlush);
        } catch (IOException exception) {
            stats.getFlushStats().registerFailedEvent(MathUtils.elapsedNanos(startTime),
                    TimeUnit.NANOSECONDS);
            throw exception;
        } finally {
            this.ledgerStorage.getFlushMutex().unlock();
            flushMutex.unlock();
        }
    }

    @SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
    public synchronized void shutdown() throws InterruptedException {
        LOG.info("Shutting down ColdLedgerStorageArchiver");
        // Interrupt GC executor thread
        gcExecutor.shutdownNow();
        try {
            entryLogMetaMap.close();
            if (writeCacheForDowngrade != null) {
                writeCacheForDowngrade.close();
            }
        } catch (Exception e) {
            LOG.warn("Failed to close entryLog metadata-map", e);
        }
    }

    public interface ArchiverThreadListener {
        /**
         * Rotate a new entry log to write.
         */
        void onArchiveEntryLog(long entryLogId);
    }

    public void addListener(ArchiverThreadListener listener) {
        if (null != listener) {
            listeners.add(listener);
        }
    }

    class ArchiveScannerFactory {
        EntryLogScanner newScanner(final EntryLogMetadata meta) {

            return new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    try {
                        return meta.containsLedger(ledgerId);
                    } catch (Exception e) {
                        return false;
                    }
                }

                @Override
                public void process(final long ledgerId, long offset, ByteBuf entry) throws IOException {
                    int bytesRead = entry.readableBytes();
                    if (throttler != null) {
                        throttler.acquire(bytesRead);
                    }
                    addEntry(entry);
                    stats.getArchivedEntryLogSize().addCount(bytesRead);
                }
            };
        }
    }

    static class Throttler {
        private final RateLimiter rateLimiter;

        Throttler(int throttleBytes) {
            this.rateLimiter = RateLimiter.create(throttleBytes);
        }

        // reset rate of limiter before compact one entry log file
        void resetRate(int throttleBytes) {
            this.rateLimiter.setRate(throttleBytes);
        }

        // get rate of limiter for unit test
        double getRate() {
            return this.rateLimiter.getRate();
        }

        // acquire. if bybytes: bytes of this entry; if byentries: 1.
        void acquire(int permits) {
            rateLimiter.acquire(permits);
        }
    }
}
