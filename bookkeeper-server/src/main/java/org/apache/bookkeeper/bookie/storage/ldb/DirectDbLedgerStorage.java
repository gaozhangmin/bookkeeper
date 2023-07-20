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

package org.apache.bookkeeper.bookie.storage.ldb;


import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.bookie.DefaultEntryLogger.fileName2LogId;
import static org.apache.bookkeeper.bookie.DefaultEntryLogger.readLastLogId;
import static org.apache.bookkeeper.bookie.DefaultEntryLogger.readLogIdByScan;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.DefaultEntryLogger;
import org.apache.bookkeeper.bookie.DiskCacheDownGradeStatus;
import org.apache.bookkeeper.bookie.EntryLoggerAllocator;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.bookie.JournalAliveListener;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class DirectDbLedgerStorage implements LedgerStorage {
    private static final Logger LOG = LoggerFactory.getLogger(DirectDbLedgerStorage.class);
    static final long INVALID_LID = -1L;
    static final String READ_AHEAD_CACHE_BATCH_SIZE = "dbStorage_readAheadCacheBatchSize";
    static final String READ_AHEAD_CACHE_BATCH_BYTES_SIZE = "dbStorage_readAheadCacheBatchBytesSize";
    public static final String READ_AHEAD_CACHE_MAX_SIZE_MB = "dbStorage_readAheadCacheMaxSizeMb";
    private static final String MAX_READAHEAD_BATCH_SIZE = "readahead-max-batch-size";
    public static final String WRITE_CACHE_MAX_SIZE_MB = "dbStorage_writeCacheMaxSizeMb";
    public static final int MB = 1024 * 1024;
    private static final long DEFAULT_READ_CACHE_MAX_SIZE_MB =
            (long) (0.25 * PlatformDependent.estimateMaxDirectMemory()) / MB;
    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE_MB =
            (long) (0.25 * PlatformDependent.estimateMaxDirectMemory()) / MB;
    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE = 100;
    // the default value is -1. this feature(limit of read ahead bytes) is disabled
    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_BYTES_SIZE = -1;
    protected static final long STORAGE_FLAGS_KEY = 0L;

    protected  ServerConfiguration conf;
    private List<DirectDbSingleLedgerStorage> ledgerStorageList;

    protected int numberOfDirs;
    protected int numberOfColdDirs;
    protected ExecutorService entryLoggerWriteExecutor = null;
    protected ExecutorService entryLoggerFlushExecutor = null;
    protected ByteBufAllocator allocator;



    @StatsDoc(
            name = MAX_READAHEAD_BATCH_SIZE,
            help = "the configured readahead batch size"
    )
    private Gauge<Integer> readaheadBatchSizeGauge;

    protected StatsLogger statsLogger;
    protected LedgerManager ledgerManager;

    public void initializeWithCold(ServerConfiguration conf, LedgerManager ledgerManager,
                                   LedgerDirsManager ledgerDirsManager,
                                   LedgerDirsManager indexDirsManager,
                                   LedgerDirsManager coldLedgerDirsManager,
                                   StatsLogger statsLogger,
                                   ByteBufAllocator allocator) throws IOException {
        checkArgument(coldLedgerDirsManager != null,
                "DirectDbLedgerStorage implementation only allows for coldLedgerDirectories configured");
        long writeCacheMaxSize = getLongVariableOrDefault(conf, WRITE_CACHE_MAX_SIZE_MB,
                DEFAULT_WRITE_CACHE_MAX_SIZE_MB) * MB;
        long readCacheMaxSize = getLongVariableOrDefault(conf, READ_AHEAD_CACHE_MAX_SIZE_MB,
                DEFAULT_READ_CACHE_MAX_SIZE_MB) * MB;
        this.allocator = allocator;
        this.numberOfDirs = ledgerDirsManager.getAllLedgerDirs().size();
        this.numberOfColdDirs = coldLedgerDirsManager.getAllLedgerDirs().size();
        this.conf = conf;
        this.statsLogger = statsLogger;
        this.ledgerManager =ledgerManager;
        ledgerStorageList = Lists.newArrayList();
        LOG.info("Started Db Ledger Storage");
        LOG.info(" - Number of directories: {}. Number of cold directories: {}", numberOfDirs, numberOfColdDirs);
        LOG.info(" - Write cache size: {} MB", writeCacheMaxSize / MB);
        LOG.info(" - Read Cache: {} MB", readCacheMaxSize / MB);

        if (readCacheMaxSize + writeCacheMaxSize > PlatformDependent.estimateMaxDirectMemory()) {
            throw new IOException("Read and write cache sizes exceed the configured max direct memory size");
        }
        int numColdLedgerDirs = coldLedgerDirsManager.getAllLedgerDirs().size();
        long perDirectoryReadCacheSize = readCacheMaxSize / numColdLedgerDirs;
        long perDirectoryWriteCacheSize = writeCacheMaxSize / numberOfColdDirs;
        int readAheadCacheBatchSize = conf.getInt(READ_AHEAD_CACHE_BATCH_SIZE, DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE);
        long readAheadCacheBatchBytesSize = conf.getInt(READ_AHEAD_CACHE_BATCH_BYTES_SIZE,
                DEFAULT_READ_AHEAD_CACHE_BATCH_BYTES_SIZE);

        for (int i = 0; i < ledgerDirsManager.getAllLedgerDirs().size(); i++) {
            File ledgerDir = ledgerDirsManager.getAllLedgerDirs().get(i);
            File indexDir = indexDirsManager.getAllLedgerDirs().get(i);
            File coldLedgerDir = coldLedgerDirsManager.getAllLedgerDirs().get(i);

            // Create a ledger dirs manager for the single directory
            File[] lDirs = new File[1];
            // Remove the `/current` suffix which will be appended again by LedgersDirManager
            lDirs[0] = ledgerDir.getParentFile();
            LedgerDirsManager ldm = new LedgerDirsManager(conf, lDirs, ledgerDirsManager.getDiskChecker(),
                    NullStatsLogger.INSTANCE);

            // Create a cold ledger dirs manager for the single directory

            // Create a cold ledger dirs manager for the single directory
            File[] cDir = new File[1];
            // Remove the `/current` suffix which will be appended again by LedgersDirManager
            cDir[0] = coldLedgerDir.getParentFile();
            LedgerDirsManager cdm = new LedgerDirsManager(conf, cDir, coldLedgerDirsManager.getDiskChecker(),
                    NullStatsLogger.INSTANCE);

            // Create an index dirs manager for the single directory
            File[] iDirs = new File[1];
            // Remove the `/current` suffix which will be appended again by LedgersDirManager
            iDirs[0] = indexDir.getParentFile();
            LedgerDirsManager idm = new LedgerDirsManager(conf, iDirs, indexDirsManager.getDiskChecker(),
                    NullStatsLogger.INSTANCE);
            ConcurrentLongHashSet cachedEntryLogIdsCache = getCachedEntryLogIdsCache(ledgerDir);
            long lastLogId = getLastLogId(ledgerDir, coldLedgerDir);
            DefaultEntryLogger entrylogger =
                    new DefaultEntryLogger(conf, ldm, null, statsLogger, allocator, lastLogId);

            EntryLoggerAllocator entryLoggerAllocator = entrylogger.getEntryLoggerAllocator();
            DefaultEntryLogger.RecentEntryLogsStatus recentEntryLogsStatus =
                    entrylogger.getRecentlyCreatedEntryLogsStatus();

            EntryLogger coldEntrylogger = new DefaultEntryLogger(conf, cdm, null, statsLogger,
                    allocator, recentEntryLogsStatus, entryLoggerAllocator);


            ledgerStorageList.add(newDirectDbSingleLedgerStorage(conf, ledgerManager, cachedEntryLogIdsCache,
                    ldm, idm, cdm, entrylogger, coldEntrylogger, statsLogger, perDirectoryWriteCacheSize,
                    perDirectoryReadCacheSize, readAheadCacheBatchSize, readAheadCacheBatchBytesSize));
            ldm.getListeners().forEach(ledgerDirsManager::addLedgerDirsListener);
            cdm.getListeners().forEach(coldLedgerDirsManager::addLedgerDirsListener);
            if (!lDirs[0].getPath().equals(iDirs[0].getPath())) {
                idm.getListeners().forEach(indexDirsManager::addLedgerDirsListener);
            }
        }
        // parent DbLedgerStorage stats (not per directory)
        readaheadBatchSizeGauge = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return readAheadCacheBatchSize;
            }

            @Override
            public Integer getSample() {
                return readAheadCacheBatchSize;
            }
        };
        statsLogger.registerGauge(MAX_READAHEAD_BATCH_SIZE, readaheadBatchSizeGauge);
    }

    private long getLastLogId(File dir, File coldDir) {
        long id = readLastLogId(dir);
        // read success
        if (id > 0) {
            return id;
        }
        long lastId = readLogIdByScan(dir);
        long lastColdId = readLogIdByScan(coldDir);
        return Math.max(lastId, lastColdId);
    }

    private ConcurrentLongHashSet getCachedEntryLogIdsCache(File dir) {
        File[] logFiles = dir.listFiles(file -> file.getName().endsWith(".log"));
        ConcurrentLongHashSet cachedEntryLogIdsCache = ConcurrentLongHashSet.newBuilder().build();
        if (logFiles != null) {
            for (File lf : logFiles) {
                long logId = fileName2LogId(lf.getName());
                cachedEntryLogIdsCache.add(logId);
            }
        }
        return cachedEntryLogIdsCache;
    }

    @Override
    public long addEntry(ByteBuf entry, boolean ackBeforeSync,
                         BookkeeperInternalCallbacks.WriteCallback cb, Object ctx)
            throws InterruptedException, IOException, BookieException {
        long ledgerId = entry.getLong(entry.readerIndex());
        return ledgerStorageList.get(MathUtils.signSafeMod(ledgerId, numberOfDirs))
                .addEntry(entry, ackBeforeSync, cb, ctx);
    }

    @VisibleForTesting
    protected DirectDbSingleLedgerStorage newDirectDbSingleLedgerStorage(ServerConfiguration conf,
                                                                         LedgerManager ledgerManager,
                                                                         ConcurrentLongHashSet cachedEntryLogIdsCache,
                                                                         LedgerDirsManager ledgerDirsManager,
                                                                         LedgerDirsManager indexDirsManager,
                                                                         LedgerDirsManager coldLedgerDirsManager,
                                                                         EntryLogger entryLogger,
                                                                         EntryLogger coldEntryLogger,
                                                                         StatsLogger statsLogger,
                                                                         long writeCacheSize, long readCacheSize,
                                                                         int readAheadCacheBatchSize,
                                                                         long readAheadCacheBatchBytesSize)
            throws IOException {
        return new DirectDbSingleLedgerStorage(conf, ledgerManager, cachedEntryLogIdsCache, ledgerDirsManager,
                indexDirsManager, coldLedgerDirsManager, entryLogger, coldEntryLogger, statsLogger,
                allocator, writeCacheSize, readCacheSize, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
    }
    public void setExitListener(JournalAliveListener aliveListener) {
        ledgerStorageList.forEach(s -> s.setExitListener(aliveListener));
    }
    @Override
    public void setStateManager(StateManager stateManager) {
        ledgerStorageList.forEach(s -> s.setStateManager(stateManager));
    }
    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) {
        ledgerStorageList.forEach(s -> s.setCheckpointSource(checkpointSource));
    }
    @Override
    public void setCheckpointer(Checkpointer checkpointer) {
        ledgerStorageList.forEach(s -> s.setCheckpointer(checkpointer));
    }

    @Override
    public void start() {
        ledgerStorageList.forEach(DirectDbSingleLedgerStorage::start);
    }

    @Override
    public void shutdown() throws InterruptedException {
        for (LedgerStorage ls : ledgerStorageList) {
            ls.shutdown();
        }

        if (entryLoggerWriteExecutor != null) {
            entryLoggerWriteExecutor.shutdown();
        }
        if (entryLoggerFlushExecutor != null) {
            entryLoggerFlushExecutor.shutdown();
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).ledgerExists(ledgerId);
    }

    @Override
    public boolean entryExists(long ledgerId, long entryId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).entryExists(ledgerId, entryId);
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        getLedgerStorage(ledgerId).setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).readMasterKey(ledgerId);
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException, BookieException {
        long ledgerId = entry.getLong(entry.readerIndex());
        return getLedgerStorage(ledgerId).addEntry(entry);
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getEntry(ledgerId, entryId);
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getLastAddConfirmed(ledgerId);
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId, long previousLAC,
                                                 Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        return getLedgerStorage(ledgerId).waitForLastAddConfirmedUpdate(ledgerId, previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        getLedgerStorage(ledgerId).cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
    }

    @Override
    public void flush() throws IOException {
        for (LedgerStorage ls : ledgerStorageList) {
            ls.flush();
        }
    }

    @Override
    public void checkpoint(CheckpointSource.Checkpoint checkpoint) throws IOException {
        for (LedgerStorage ls : ledgerStorageList) {
            ls.checkpoint(checkpoint);
        }
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        getLedgerStorage(ledgerId).deleteLedger(ledgerId);
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        ledgerStorageList.forEach(ls -> ls.registerLedgerDeletionListener(listener));
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        getLedgerStorage(ledgerId).setExplicitLac(ledgerId, lac);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getExplicitLac(ledgerId);
    }

    public long addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
                                 LedgerCache.PageEntriesIterable pages) throws Exception {
        return getLedgerStorage(ledgerId).addLedgerToIndex(ledgerId, isFenced, masterKey, pages);
    }

    public long getLastEntryInLedger(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).getEntryLocationIndex().getLastEntryInLedger(ledgerId);
    }

    @Override
    public boolean downgrading(long ledgerId) {
        return ledgerStorageList.get(MathUtils.signSafeMod(ledgerId, numberOfDirs)).downgrading(ledgerId);
    }

    public long getLocation(long ledgerId, long entryId) throws IOException {
        return getLedgerStorage(ledgerId).getEntryLocationIndex().getLocation(ledgerId, entryId);
    }

    private DirectDbSingleLedgerStorage getLedgerStorage(long ledgerId) {
        return ledgerStorageList.get(MathUtils.signSafeMod(ledgerId, numberOfDirs));
    }

    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        List<Iterable<Long>> listIt = new ArrayList<>(numberOfDirs);
        for (DirectDbSingleLedgerStorage ls : ledgerStorageList) {
            listIt.add(ls.getActiveLedgersInRange(firstLedgerId, lastLedgerId));
        }

        return Iterables.concat(listIt);
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).getLastEntry(ledgerId);
    }


    @VisibleForTesting
    List<DirectDbSingleLedgerStorage> getLedgerStorageList() {
        return ledgerStorageList;
    }

    @Override
    public void forceGC() {
        ledgerStorageList.stream().forEach(DirectDbSingleLedgerStorage::forceGC);
    }

    @Override
    public void forceGC(boolean forceMajor, boolean forceMinor,
                        double majorCompactionThreshold, double minorCompactionThreshold,
                        long majorCompactionMaxTimeMillis, long minorCompactionMaxTimeMillis) {
        ledgerStorageList.stream().forEach(s -> s.forceGC(forceMajor, forceMinor, majorCompactionThreshold,
                minorCompactionThreshold, majorCompactionMaxTimeMillis, minorCompactionMaxTimeMillis));
    }

    @Override
    public boolean isInForceGC() {
        return ledgerStorageList.stream().anyMatch(DirectDbSingleLedgerStorage::isInForceGC);
    }

    @Override
    public void suspendMinorGC() {
        ledgerStorageList.stream().forEach(DirectDbSingleLedgerStorage::suspendMinorGC);
    }

    @Override
    public void suspendMajorGC() {
        ledgerStorageList.stream().forEach(DirectDbSingleLedgerStorage::suspendMajorGC);
    }

    @Override
    public void resumeMinorGC() {
        ledgerStorageList.stream().forEach(DirectDbSingleLedgerStorage::resumeMinorGC);
    }

    @Override
    public void resumeMajorGC() {
        ledgerStorageList.stream().forEach(DirectDbSingleLedgerStorage::resumeMajorGC);
    }

    @Override
    public boolean isMajorGcSuspended() {
        return ledgerStorageList.stream().allMatch(DirectDbSingleLedgerStorage::isMajorGcSuspended);
    }

    @Override
    public boolean isMinorGcSuspended() {
        return ledgerStorageList.stream().allMatch(DirectDbSingleLedgerStorage::isMinorGcSuspended);
    }

    @Override
    public void entryLocationCompact() {
        ledgerStorageList.forEach(DirectDbSingleLedgerStorage::entryLocationCompact);
    }

    @Override
    public void entryLocationCompact(List<String> locations) {
        for (DirectDbSingleLedgerStorage ledgerStorage : ledgerStorageList) {
            String entryLocation = ledgerStorage.getEntryLocationDBPath().get(0);
            if (locations.contains(entryLocation)) {
                ledgerStorage.entryLocationCompact();
            }
        }
    }

    @Override
    public boolean isEntryLocationCompacting() {
        return ledgerStorageList.stream().anyMatch(DirectDbSingleLedgerStorage::isEntryLocationCompacting);
    }

    @Override
    public Map<String, Boolean> isEntryLocationCompacting(List<String> locations) {
        HashMap<String, Boolean> isCompacting = Maps.newHashMap();
        for (DirectDbSingleLedgerStorage ledgerStorage : ledgerStorageList) {
            String entryLocation = ledgerStorage.getEntryLocationDBPath().get(0);
            if (locations.contains(entryLocation)) {
                isCompacting.put(entryLocation, ledgerStorage.isEntryLocationCompacting());
            }
        }
        return isCompacting;
    }

    @Override
    public List<String> getEntryLocationDBPath() {
        List<String> allEntryLocationDBPath = Lists.newArrayList();
        for (DirectDbSingleLedgerStorage ledgerStorage : ledgerStorageList) {
            allEntryLocationDBPath.addAll(ledgerStorage.getEntryLocationDBPath());
        }
        return allEntryLocationDBPath;
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return ledgerStorageList.stream()
                .map(single -> single.getGarbageCollectionStatus().get(0)).collect(Collectors.toList());
    }

    @Override
    public List<DiskCacheDownGradeStatus> getDiskCacheDowngradeStatus() {
        return ledgerStorageList.stream()
                .map(single -> single.getDiskCacheDowngradeStatus().get(0)).collect(Collectors.toList());
    }

    public static long getLongVariableOrDefault(ServerConfiguration conf, String keyName, long defaultValue) {
        Object obj = conf.getProperty(keyName);
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        } else if (obj == null) {
            return defaultValue;
        } else if (StringUtils.isEmpty(conf.getString(keyName))) {
            return defaultValue;
        } else {
            return conf.getLong(keyName);
        }
    }

    public static boolean getBooleanVariableOrDefault(ServerConfiguration conf, String keyName, boolean defaultValue) {
        Object obj = conf.getProperty(keyName);
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj == null) {
            return defaultValue;
        } else if (StringUtils.isEmpty(conf.getString(keyName))) {
            return defaultValue;
        } else {
            return conf.getBoolean(keyName);
        }
    }

    @Override
    public PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        // check Issue #2078
        throw new UnsupportedOperationException(
                "getListOfEntriesOfLedger method is currently unsupported for DbLedgerStorage");
    }

    @Override
    public void setLimboState(long ledgerId) throws IOException {
        getLedgerStorage(ledgerId).setLimboState(ledgerId);
    }

    @Override
    public boolean hasLimboState(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).hasLimboState(ledgerId);
    }

    @Override
    public void clearLimboState(long ledgerId) throws IOException {
        getLedgerStorage(ledgerId).clearLimboState(ledgerId);
    }

    @Override
    public EnumSet<StorageState> getStorageStateFlags() throws IOException {
        return getLedgerStorage(STORAGE_FLAGS_KEY).getStorageStateFlags();
    }

    @Override
    public void setStorageStateFlag(StorageState flag) throws IOException {
        getLedgerStorage(STORAGE_FLAGS_KEY).setStorageStateFlag(flag);
    }

    @Override
    public void clearStorageStateFlag(StorageState flag) throws IOException {
        getLedgerStorage(STORAGE_FLAGS_KEY).clearStorageStateFlag(flag);
    }


}
