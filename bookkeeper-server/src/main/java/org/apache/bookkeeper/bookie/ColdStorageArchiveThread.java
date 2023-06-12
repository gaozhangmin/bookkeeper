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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.bookie.BookieException.EntryLogMetadataMapException;
import org.apache.bookkeeper.bookie.stats.ColdStorageArchiveStats;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.ldb.PersistentEntryLogMetadataMap;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.bookkeeper.util.BookKeeperConstants.METADATA_CACHE;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class ColdStorageArchiveThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ColdStorageArchiveThread.class);
    private static final String LOG_FILE_SUFFIX = ".log";

    private final long archiveIntervalMs;

    private final File coldLedgerDir;

    private final File ledgerDir;

    private final int archiveReadBufferSize;
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

    final CompactableLedgerStorage ledgerStorage;

    volatile boolean running = true;
    final ServerConfiguration conf;
    final LedgerDirsManager ledgerDirsManager;
    final ScanAndCompareGarbageCollector garbageCollector;

    final AbstractLogCompactor.Throttler scanThrottler;

    final AtomicBoolean forceArchive = new AtomicBoolean(false);
    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public ColdStorageArchiveThread(ServerConfiguration conf, LedgerManager ledgerManager,
                                    final LedgerDirsManager ledgerDirsManager,
                                    final LedgerDirsManager coldLedgerDirsManager,
                                    final CompactableLedgerStorage ledgerStorage,
                                    EntryLogger entryLogger,
                                    EntryLogger coldEntryLogger, StatsLogger statsLogger) throws IOException {
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("ColdStorageArchiveThread"));
        this.conf = conf;

        this.ledgerDirsManager = ledgerDirsManager;
        this.entryLogger = entryLogger;
        this.entryLogMetaMap = createEntryLogMetadataMap();
        this.ledgerStorage = ledgerStorage;

        this.numActiveEntryLogs = 0;
        this.totalEntryLogSize = 0L;
        this.garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, ledgerStorage, conf, statsLogger);
        this.stats = new ColdStorageArchiveStats(
                statsLogger,
                () -> numActiveEntryLogs,
                () -> totalEntryLogSize
        );
        this.scanThrottler = new AbstractLogCompactor.Throttler(conf);

        this.archiveIntervalMs = conf.getColdStorageArchiveInterval();
        this.coldLedgerDir = coldLedgerDirsManager.getAllLedgerDirs().get(0);
        this.ledgerDir = ledgerDirsManager.getAllLedgerDirs().get(0);
        this.archiveReadBufferSize = conf.getArchiveReadBufferSize();
        this.coldEntryLogger = coldEntryLogger;
        int archiveRateByBytes = conf.getArchiveRateBytes();
        if (archiveRateByBytes > 0) {
            this.throttler = new Throttler(archiveRateByBytes);
        }
        this.ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
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
                enableForceArchive();
            }

            @Override
            public void diskWritable(File disk) {
                // we have enough space now
               disableForceArchive();
            }
        };
    }

    @VisibleForTesting
    EntryLogMetadataMap getEntryLogMetaMap() {
        return entryLogMetaMap;
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
            extractMetaFromEntryLogs();
            stats.getExtractionRunTime().registerSuccessfulEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);

            archiveToColdStorage();

            stats.getArchiveThreadRuntime().registerSuccessfulEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);
        } catch (EntryLogMetadataMapException e) {
            LOG.error("Error in entryLog-metadatamap, Failed to archive  due to entry-log {}",
                    e.getMessage(), e);
            stats.getArchiveThreadRuntime().registerFailedEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);
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
        // remove entry log file successfully
        coldEntryLogger.flushColdEntrylogger(entryLogId);
        entryLogger.archivedEntryLog(entryLogId);
        if (entryLogger.removeEntryLog(entryLogId)) {
            LOG.info("Removing EntryLog={}-{} After archiving", entryLogId, Long.toHexString(entryLogId));
            entryLogMetaMap.remove(entryLogId);
        }
    }

    protected void removeEmptyEntryLog(long entryLogId) throws EntryLogMetadataMapException {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            LOG.info("Removing empty EntryLog={}-{}", entryLogId, Long.toHexString(entryLogId));
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
    void archiveToColdStorage() throws EntryLogMetadataMapException {
        LOG.info("archiveToColdStorage started");
        long currentTime = System.currentTimeMillis();
        if (this.throttler != null) {
            this.throttler.resetRate(this.conf.getArchiveRateBytes());
            LOG.info("Reset archive throttler to {}", this.conf.getArchiveRateBytes());
        }
        AtomicLong totalEntryLogSizeAcc = new AtomicLong(0L);
        this.entryLogMetaMap.forEach((entryLogId, meta) -> {
            if (meta == null) {
                LOG.info("ArchiveToColdStorage: Metadata for entry log {} already deleted", entryLogId);
                return;
            }
            if (!forceArchive.get() && currentTime - meta.creationTime <= conf.getWarmStorageRetentionTime()) {
                totalEntryLogSizeAcc.getAndAdd(meta.getRemainingSize());
                return;
            }
//            try {
//                File coldStorageFile = new File(coldLedgerDir, Long.toHexString(entryLogId) + LOG_FILE_SUFFIX);
//                File entrylogFile = new File(ledgerDir, Long.toHexString(entryLogId) + LOG_FILE_SUFFIX);
//                Files.copy(entrylogFile.toPath(), coldStorageFile.toPath());
//                this.removeEntryLogByArchive(entryLogId);
//                stats.getReclaimedDiskCacheSpaceViaArchive().addCount(meta.getTotalSize());
//                LOG.info("LedgerDir={}, coldLedgerDir={}, Archived entryLog={}-{}",
//                        ledgerDir, coldLedgerDir, entryLogId, Long.toHexString(entryLogId));
//            } catch (IOException ex) {
//                LOG.warn("EntryLog={}-{} is not found during archiving process",
//                        entryLogId, Long.toHexString(entryLogId), ex);
//            } catch (EntryLogMetadataMapException e) {
//                LOG.error("Error in entryLog-metadatamap, Failed to archive entryLog={}-{}",
//                        entryLogId, Long.toHexString(entryLogId), e);
//            }

            try (InputStream inputStream = Files.newInputStream(
                    new File(ledgerDir, Long.toHexString(entryLogId) + LOG_FILE_SUFFIX).toPath());
                 OutputStream outputStream = Files.newOutputStream(
                         new File(coldLedgerDir, Long.toHexString(entryLogId) + LOG_FILE_SUFFIX).toPath())
            ) {
                this.archiveWithRateLimit(inputStream, outputStream);
                this.removeEntryLogByArchive(entryLogId);
                stats.getReclaimedDiskCacheSpaceViaArchive().addCount(meta.getTotalSize());
                LOG.info("LedgerDir={}, coldLedgerDir={}, Archived entryLog={}-{}",
                        ledgerDir, coldLedgerDir, entryLogId, Long.toHexString(entryLogId));
            } catch (IOException e) {
                LOG.warn("EntryLog={}-{} is not found during archiving process",
                        entryLogId, Long.toHexString(entryLogId), e);
            } catch (EntryLogMetadataMapException e) {
                LOG.error("Error in entryLog-metadatamap, Failed to archive entryLog={}-{}",
                        entryLogId, Long.toHexString(entryLogId), e);
            }

        });
        entryLogger.archivedLogIds();
        LOG.info("LedgerDir={}, coldLedgerDir={}, archiveToColdStorage finished, archivedMaxLogId={}-{}",
                ledgerDir, coldLedgerDir, entryLogger.getMaxArchivedLogId(),
                Long.toHexString(entryLogger.getMaxArchivedLogId()));
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
                    BasicFileAttributes attributes = Files.readAttributes(
                            new File(ledgerDir, Long.toHexString(entryLogId) + ".log").toPath(),
                            BasicFileAttributes.class);
                    long creationTime = attributes.creationTime().toMillis();
                    entryLogMeta.setCreationTime(creationTime);
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

    private void archiveWithRateLimit(InputStream inputStream, OutputStream outputStream)
            throws IOException {
        byte[] buffer = new byte[archiveReadBufferSize];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer, 0, buffer.length)) >= 0) {
            if (throttler != null) {
                throttler.acquire(bytesRead);
            }
            outputStream.write(buffer, 0, bytesRead);
        }
    }

    @SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
    public synchronized void shutdown() throws InterruptedException {
        if (!this.running) {
            return;
        }
        LOG.info("Shutting down ColdLedgerStorageArchiver");
        this.running = false;
        // Interrupt GC executor thread
        gcExecutor.shutdownNow();
        try {
            entryLogMetaMap.close();
        } catch (Exception e) {
            LOG.warn("Failed to close entryLog metadata-map", e);
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
