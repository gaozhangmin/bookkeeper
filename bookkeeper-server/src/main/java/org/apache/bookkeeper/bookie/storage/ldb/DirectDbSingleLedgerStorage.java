package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.bookie.DefaultEntryLogger.logIdForOffset;
import static org.apache.bookkeeper.bookie.DefaultEntryLogger.posForOffset;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.ColdStorageArchiveThread;
import org.apache.bookkeeper.bookie.CompactableLedgerStorage;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.apache.bookkeeper.common.collections.BatchedBlockingQueue;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookieRequestHandler;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DirectDbSingleLedgerStorage extends BookieCriticalThread implements CompactableLedgerStorage {
    private static final Logger LOG = LoggerFactory.getLogger(DirectDbSingleLedgerStorage.class);

    protected final EntryLogger entryLogger;
    protected EntryLogger coldEntryLogger;

    protected final LedgerMetadataIndex ledgerIndex;
    protected final EntryLocationIndex entryLocationIndex;

    private final ConcurrentLongHashMap<TransientLedgerInfo> transientLedgerInfoCache;

    private ColdStorageArchiveThread coldStorageBackupThread = null;

    private final GarbageCollectorThread gcThread;

    private static final String THREAD_NAME = "BookieDirectStorage";
//    private final ExecutorService executor = Executors.newSingleThreadExecutor(
//            new DefaultThreadFactory("db-storage"));

    // Executor used to for db index cleanup
    private final ScheduledExecutorService cleanupExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("db-storage-cleanup"));

    private final CopyOnWriteArrayList<LedgerDeletionListener> ledgerDeletionListeners = Lists
            .newCopyOnWriteArrayList();
    private final DirectDBLedgerStorageStats dbLedgerStorageStats;
    final BatchedBlockingQueue<QueueEntry> queue;
    final BatchedBlockingQueue<ForceWriteRequest> forceWriteRequests;
    final ServerConfiguration conf;
    private static final RecyclableArrayList.Recycler<QueueEntry> entryListRecycler =
            new RecyclableArrayList.Recycler<QueueEntry>();
    // Time after which we will stop grouping and issue the flush
    private final long maxGroupWaitInNanos;
    // should we flush if the queue is empty
    private final boolean flushWhenQueueEmpty;
    // Threshold after which we flush any buffered  entries
    private final long bufferedEntriesThreshold;
    volatile boolean running = true;
    final ForceWriteThread forceWriteThread;
    private final String ledgerBaseDir;
    private String coldLedgerBaseDir = null;
    private final Counter callbackTime;
    private final ReadCache readCache;
    private final long readCacheMaxSize;
    private final int readAheadCacheBatchSize;
    private final long readAheadCacheBatchBytesSize;
    private final long maxReadAheadBytesSize;

    private final AtomicBoolean downgradeToColdStorage = new AtomicBoolean(false);

    private final LedgerDirsManager ledgerDirsManager;
    private final LedgerDirsManager indexDirsManager;

    public DirectDbSingleLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                                       LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
                                       LedgerDirsManager coldLedgerDirsManager, EntryLogger entryLogger,
                                       EntryLogger coldEntryLogger, StatsLogger statsLogger,
                                       ByteBufAllocator allocator, long readCacheSize, int readAheadCacheBatchSize,
                                       long readAheadCacheBatchBytesSize) throws IOException {
        super(THREAD_NAME + "-" + conf.getBookiePort());
        checkArgument(ledgerDirsManager.getAllLedgerDirs().size() == 1,
                "Db implementation only allows for one storage dir");
        this.conf = conf;
        this.readCacheMaxSize = readCacheSize;
        this.ledgerDirsManager = ledgerDirsManager;
        this.indexDirsManager = indexDirsManager;
        this.readAheadCacheBatchSize = readAheadCacheBatchSize;
        this.readAheadCacheBatchBytesSize = readAheadCacheBatchBytesSize;
        // Do not attempt to perform read-ahead more than half the total size of the cache
        maxReadAheadBytesSize = readCacheMaxSize / 2;

        readCache = new ReadCache(allocator, readCacheMaxSize);
        String ledgerBaseDir = ledgerDirsManager.getAllLedgerDirs().get(0).getPath();
        this.ledgerBaseDir = ledgerBaseDir;
        // indexBaseDir default use ledgerBaseDir
        String indexBaseDir = ledgerBaseDir;
        if (CollectionUtils.isEmpty(indexDirsManager.getAllLedgerDirs())) {
            LOG.info("indexDir is not specified, use default, creating single directory db ledger storage on {}",
                    indexBaseDir);
        } else {
            // if indexDir is specified, set new value
            indexBaseDir = indexDirsManager.getAllLedgerDirs().get(0).getPath();
            LOG.info("indexDir is specified, creating single directory db ledger storage on {}", indexBaseDir);
        }

        StatsLogger ledgerIndexDirStatsLogger = statsLogger
                .scopeLabel("ledgerDir", ledgerBaseDir)
                .scopeLabel("indexDir", indexBaseDir);
        if (coldLedgerDirsManager != null) {
            String coldLedgerBaseDir = coldLedgerDirsManager.getAllLedgerDirs().get(0).getPath();
            this.coldLedgerBaseDir = coldLedgerBaseDir;
            ledgerIndexDirStatsLogger
                    = ledgerIndexDirStatsLogger.scopeLabel("coldLedgerDir", coldLedgerBaseDir);
        }

        ledgerIndex = new LedgerMetadataIndex(conf,
                KeyValueStorageRocksDB.factory, indexBaseDir, ledgerIndexDirStatsLogger);
        entryLocationIndex = new EntryLocationIndex(conf,
                KeyValueStorageRocksDB.factory, indexBaseDir, ledgerIndexDirStatsLogger);

        transientLedgerInfoCache = ConcurrentLongHashMap.<TransientLedgerInfo>newBuilder()
                .expectedItems(16 * 1024)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
                .build();
        cleanupExecutor.scheduleAtFixedRate(this::cleanupStaleTransientLedgerInfo,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES, TimeUnit.MINUTES);
        this.entryLogger = entryLogger;
        if (coldEntryLogger != null && coldLedgerDirsManager != null) {
            this.coldEntryLogger = coldEntryLogger;
            coldStorageBackupThread = new ColdStorageArchiveThread(conf, ledgerManager, ledgerDirsManager,
                    coldLedgerDirsManager, this, entryLogger, coldEntryLogger, ledgerIndexDirStatsLogger,
                    allocator);
            gcThread = new GarbageCollectorThread(conf, ledgerManager, coldLedgerDirsManager, this,
                    coldEntryLogger, ledgerIndexDirStatsLogger);
            coldLedgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
            ledgerDirsManager.addLedgerDirsListener(getDiskCacheDirsListener());
        } else {
            gcThread = new GarbageCollectorThread(conf,
                    ledgerManager, ledgerDirsManager, this, entryLogger, ledgerIndexDirStatsLogger);
            ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        }

        if (conf.isBusyWaitEnabled()) {
            // To achieve lower latency, use busy-wait blocking queue implementation
            queue = new BlockingMpscQueue<>(conf.getDirectStorageQueueSize());
            forceWriteRequests = new BlockingMpscQueue<>(conf.getDirectStorageQueueSize());
        } else {
            queue = new BatchedArrayBlockingQueue<>(conf.getDirectStorageQueueSize());
            forceWriteRequests = new BatchedArrayBlockingQueue<>(conf.getDirectStorageQueueSize());
        }

        dbLedgerStorageStats = new DirectDBLedgerStorageStats(ledgerIndexDirStatsLogger,
                readCache::size, readCache::count);
        this.maxGroupWaitInNanos = TimeUnit.MILLISECONDS.toNanos(conf.getDirectStorageMaxGroupWaitMSec());// Unless there is a cap on the max wait (which requires group force writes)
        // we cannot skip flushing for queue empty
        this.flushWhenQueueEmpty = maxGroupWaitInNanos <= 0 || conf.getDirectStorageFlushWhenQueueEmpty();
        this.bufferedEntriesThreshold = conf.getDirectStorageBufferedEntriesThreshold();
        this.forceWriteThread = new ForceWriteThread(this);
        this.callbackTime = ledgerIndexDirStatsLogger.getThreadScopedCounter("callback-time");

        if (!ledgerBaseDir.equals(indexBaseDir)) {
            indexDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        }
    }

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager, LedgerDirsManager coldLedgerDirsManager,
                           StatsLogger statsLogger, ByteBufAllocator allocator) throws IOException {
        /// Initialized in constructor
    }

    @Override
    public void setStateManager(StateManager stateManager) { }

    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) { }
    @Override
    public void setCheckpointer(Checkpointer checkpointer) { }

    @Override
    public void forceGC() {
        gcThread.enableForceGC();
    }

    @Override
    public void forceGC(boolean forceMajor, boolean forceMinor) {
        gcThread.enableForceGC(forceMajor, forceMinor);
    }

    @Override
    public boolean isInForceGC() {
        return gcThread.isInForceGC();
    }

    public void suspendMinorGC() {
        gcThread.suspendMinorGC();
    }

    public void suspendMajorGC() {
        gcThread.suspendMajorGC();
    }

    public void resumeMinorGC() {
        gcThread.resumeMinorGC();
    }

    public void resumeMajorGC() {
        gcThread.resumeMajorGC();
    }

    public boolean isMajorGcSuspended() {
        return gcThread.isMajorGcSuspend();
    }

    public boolean isMinorGcSuspended() {
        return gcThread.isMinorGcSuspend();
    }

    @Override
    public void entryLocationCompact() {
        if (entryLocationIndex.isCompacting()) {
            // RocksDB already running compact.
            return;
        }
        cleanupExecutor.execute(() -> {
            // There can only be one single cleanup task running because the cleanupExecutor
            // is single-threaded
            try {
                LOG.info("Trigger entry location index RocksDB compact.");
                entryLocationIndex.compact();
            } catch (Throwable t) {
                LOG.warn("Failed to trigger entry location index RocksDB compact", t);
            }
        });
    }

    @Override
    public boolean isEntryLocationCompacting() {
        return entryLocationIndex.isCompacting();
    }

    @Override
    public List<String> getEntryLocationDBPath() {
        return Lists.newArrayList(entryLocationIndex.getEntryLocationDBPath());
    }

    public void shutdown() throws InterruptedException {
        try {
            if (!running) {
                return;
            }
            LOG.info("Shutting down bookie direct ledgerStorage");
            flush();
            gcThread.shutdown();
            forceWriteThread.shutdown();
            entryLogger.close();
            if (coldStorageBackupThread != null) {
                coldStorageBackupThread.shutdown();
                coldEntryLogger.close();
            }
            cleanupExecutor.shutdown();
            cleanupExecutor.awaitTermination(1, TimeUnit.SECONDS);
            ledgerIndex.close();
            entryLocationIndex.close();
//            executor.shutdown();
            readCache.close();
            running = false;
            this.interrupt();
            this.join();
            LOG.info("Finished Shutting down bookie direct ledgerStorage");
        } catch (IOException e) {
            LOG.error("Error closing db storage", e);
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        try {
            DbLedgerStorageDataFormats.LedgerData ledgerData = ledgerIndex.get(ledgerId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ledger exists. ledger: {} : {}", ledgerId, ledgerData.getExists());
            }
            return ledgerData.getExists();
        } catch (Bookie.NoLedgerException nle) {
            // ledger does not exist
            return false;
        }
    }

    @Override
    public boolean entryExists(long ledgerId, long entryId) throws IOException, BookieException {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return false;
        }

        boolean inCache = readCache.hasEntry(ledgerId, entryId);

        if (inCache) {
            return true;
        }

        // Read from main storage
        long entryLocation = entryLocationIndex.getLocation(ledgerId, entryId);
        if (entryLocation != 0) {
            return true;
        }

        // Only a negative result while in limbo equates to unknown
        throwIfLimbo(ledgerId);

        return false;
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Set fenced. ledger: {}", ledgerId);
        }
        boolean changed = ledgerIndex.setFenced(ledgerId);
        if (changed) {
            // notify all the watchers if a ledger is fenced
            TransientLedgerInfo ledgerInfo = transientLedgerInfoCache.get(ledgerId);
            if (null != ledgerInfo) {
                ledgerInfo.notifyWatchers(Long.MAX_VALUE);
            }
        }
        return changed;
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Set master key. ledger: {}", ledgerId);
        }
        ledgerIndex.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Read master key. ledger: {}", ledgerId);
        }
        return ledgerIndex.get(ledgerId).getMasterKey().toByteArray();
    }

    @Override
    public long addEntry(ByteBuf entry, boolean ackBeforeSync,
                         BookkeeperInternalCallbacks.WriteCallback cb, Object ctx)
            throws InterruptedException {
        long ledgerId = entry.getLong(entry.readerIndex());
        long entryId = entry.getLong(entry.readerIndex() + 8);
        logAddEntry(ledgerId, entryId, entry, ackBeforeSync, cb, ctx);
        return entryId;
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
        long startTime = MathUtils.nowInNano();
        try {
            ByteBuf entry = doGetEntry(ledgerId, entryId);
            dbLedgerStorageStats.getReadEntryStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            return entry;
        } catch (IOException e) {
            dbLedgerStorageStats.getReadEntryStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            throw e;
        }
    }

    @Override
    public void checkpoint(CheckpointSource.Checkpoint checkpoint) throws IOException { }

    @Override
    public void flush() throws IOException {
        entryLogger.flush();
        ledgerIndex.flush();
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting ledger {}", ledgerId);
        }
        entryLocationIndex.delete(ledgerId);
        ledgerIndex.delete(ledgerId);

        for (LedgerDeletionListener listener : ledgerDeletionListeners) {
            listener.ledgerDeleted(ledgerId);
        }

        TransientLedgerInfo tli = transientLedgerInfoCache.remove(ledgerId);
        if (tli != null) {
            tli.close();
        }
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        return ledgerIndex.getActiveLedgersInRange(firstLedgerId, lastLedgerId);
    }

    @Override
    public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        // Trigger a flush to have all the entries being compacted in the db storage
        flush();

        entryLocationIndex.updateLocations(locations);
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException, BookieException {
        throwIfLimbo(ledgerId);

        TransientLedgerInfo ledgerInfo = transientLedgerInfoCache.get(ledgerId);
        long lac = null != ledgerInfo ? ledgerInfo.getLastAddConfirmed() : TransientLedgerInfo.NOT_ASSIGNED_LAC;
        if (lac == TransientLedgerInfo.NOT_ASSIGNED_LAC) {
            ByteBuf bb = getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
            try {
                bb.skipBytes(2 * Long.BYTES); // skip ledger id and entry id
                lac = bb.readLong();
                lac = getOrAddLedgerInfo(ledgerId).setLastAddConfirmed(lac);
            } finally {
                ReferenceCountUtil.release(bb);
            }
        }
        return lac;
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId, long previousLAC,
                                                 Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        return getOrAddLedgerInfo(ledgerId).waitForLastAddConfirmedUpdate(previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        getOrAddLedgerInfo(ledgerId).cancelWaitForLastAddConfirmedUpdate(watcher);
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        TransientLedgerInfo ledgerInfo = getOrAddLedgerInfo(ledgerId);
        ledgerInfo.setExplicitLac(lac);
        ledgerIndex.setExplicitLac(ledgerId, lac);
        ledgerInfo.notifyWatchers(Long.MAX_VALUE);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) throws IOException, BookieException {
        throwIfLimbo(ledgerId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("getExplicitLac ledger {}", ledgerId);
        }
        TransientLedgerInfo ledgerInfo = getOrAddLedgerInfo(ledgerId);
        if (ledgerInfo.getExplicitLac() != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getExplicitLac ledger {} returned from TransientLedgerInfo", ledgerId);
            }
            return ledgerInfo.getExplicitLac();
        }
        DbLedgerStorageDataFormats.LedgerData ledgerData = ledgerIndex.get(ledgerId);
        if (!ledgerData.hasExplicitLac()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getExplicitLac ledger {} missing from LedgerData", ledgerId);
            }
            return null;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("getExplicitLac ledger {} returned from LedgerData", ledgerId);
        }
        ByteString persistedLac = ledgerData.getExplicitLac();
        ledgerInfo.setExplicitLac(Unpooled.wrappedBuffer(persistedLac.toByteArray()));
        return ledgerInfo.getExplicitLac();
    }

    private TransientLedgerInfo getOrAddLedgerInfo(long ledgerId) {
        return transientLedgerInfoCache.computeIfAbsent(ledgerId, l -> {
            return new TransientLedgerInfo(l, ledgerIndex);
        });
    }

    private void updateCachedLacIfNeeded(long ledgerId, long lac) {
        TransientLedgerInfo tli = transientLedgerInfoCache.get(ledgerId);
        if (tli != null) {
            tli.setLastAddConfirmed(lac);
        }
    }

    @Override
    public void flushEntriesLocationsIndex() throws IOException {
        // No-op. Location index is already flushed in updateEntriesLocations() call
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        ledgerDeletionListeners.add(listener);
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return Collections.singletonList(gcThread.getGarbageCollectionStatus());
    }

    @Override
    public PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        throw new UnsupportedOperationException(
                "getListOfEntriesOfLedger method is currently unsupported for SingleDirectoryDbLedgerStorage");
    }

    private LedgerDirsManager.LedgerDirsListener getDiskCacheDirsListener() {
        return new LedgerDirsManager.LedgerDirsListener() {

            @Override
            public void diskAlmostFull(File disk) {
                if (ledgerDirsManager.getAllLedgerDirs().contains(disk)
                        && downgradeToColdStorage.compareAndSet(false, true)) {
                    LOG.info("Disk {} is almost full. Downgrade to coldStorage", disk);
                    try {
                        entryLogger.flush();
                    } catch (IOException e) {
                        LOG.error("Error flushing entry logger during downgradeToColdStorage progress", e);
                    }
                }
            }

            @Override
            public void diskUnderWarnThreshold(File disk) {
                if (ledgerDirsManager.getAllLedgerDirs().contains(disk)
                        && downgradeToColdStorage.compareAndSet(true, false)) {
                    LOG.info("Disk {} is UnderWarnThreshold. Upgrade to disk cache", disk);
                    try {
                        coldEntryLogger.flush();
                        entryLogger.archivedLogIds();
                    } catch (IOException e) {
                        LOG.error("Error flushing coldEntryLogger during disabling downgradeToColdStorage", e);
                    }
                }
            }
        };
    }



    private LedgerDirsManager.LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsManager.LedgerDirsListener() {

            @Override
            public void diskAlmostFull(File disk) {
                if (ledgerDirsManager.getAllLedgerDirs().contains(disk)
                        || indexDirsManager.getAllLedgerDirs().contains(disk)) {
                    if (gcThread.isForceGCAllowWhenNoSpace()) {
                        gcThread.enableForceGC();
                    } else {
                        gcThread.suspendMajorGC();
                    }
                }
            }

            @Override
            public void diskUnderWarnThreshold(File disk) {
                if (ledgerDirsManager.getAllLedgerDirs().contains(disk)
                        || indexDirsManager.getAllLedgerDirs().contains(disk)) {
                    if (gcThread.isForceGCAllowWhenNoSpace()) {
                        gcThread.disableForceGC();
                    } else {
                        gcThread.resumeMajorGC();
                    }
                }
            }

            @Override
            public void diskFull(File disk) {
                if (ledgerDirsManager.getAllLedgerDirs().contains(disk)
                        || indexDirsManager.getAllLedgerDirs().contains(disk)) {
                    if (gcThread.isForceGCAllowWhenNoSpace()) {
                        gcThread.enableForceGC();
                    } else {
                        gcThread.suspendMajorGC();
                        gcThread.suspendMinorGC();
                    }
                }
            }

            @Override
            public void allDisksFull(boolean highPriorityWritesAllowed) {
                if (gcThread.isForceGCAllowWhenNoSpace()) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                    gcThread.suspendMinorGC();
                }
            }

            @Override
            public void diskWritable(File disk) {
                if (ledgerDirsManager.getAllLedgerDirs().contains(disk)
                        || indexDirsManager.getAllLedgerDirs().contains(disk)) {
                    // we have enough space now
                    if (gcThread.isForceGCAllowWhenNoSpace()) {
                        // disable force gc.
                        gcThread.disableForceGC();
                    } else {
                        // resume compaction to normal.
                        gcThread.resumeMajorGC();
                        gcThread.resumeMinorGC();
                    }
                }

            }

            @Override
            public void diskJustWritable(File disk) {
                if (ledgerDirsManager.getAllLedgerDirs().contains(disk)
                        || indexDirsManager.getAllLedgerDirs().contains(disk)) {
                    if (gcThread.isForceGCAllowWhenNoSpace()) {
                        // if a disk is just writable, we still need force gc.
                        gcThread.enableForceGC();
                    } else {
                        // still under warn threshold, only resume minor compaction.
                        gcThread.resumeMinorGC();
                    }
                }
            }
        };
    }

    @Override
    public void setLimboState(long ledgerId) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("setLimboState. ledger: {}", ledgerId);
        }
        ledgerIndex.setLimbo(ledgerId);
    }

    @Override
    public boolean hasLimboState(long ledgerId) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("hasLimboState. ledger: {}", ledgerId);
        }
        return ledgerIndex.get(ledgerId).getLimbo();
    }

    @Override
    public void clearLimboState(long ledgerId) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("clearLimboState. ledger: {}", ledgerId);
        }
        ledgerIndex.clearLimbo(ledgerId);
    }

    private void throwIfLimbo(long ledgerId) throws IOException, BookieException {
        if (hasLimboState(ledgerId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Accessing ledger({}) in limbo state, throwing exception", ledgerId);
            }
            throw BookieException.create(BookieException.Code.DataUnknownException);
        }
    }

    private ByteBuf doGetEntry(long ledgerId, long entryId) throws IOException, BookieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get Entry: {}@{}", ledgerId, entryId);
        }

        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntry(ledgerId);
        }
        ByteBuf entry;
        // Try reading from read-ahead cache
        entry = readCache.get(ledgerId, entryId);
        if (entry != null) {
            dbLedgerStorageStats.getReadCacheHitCounter().inc();
            return entry;
        }

        dbLedgerStorageStats.getReadCacheMissCounter().inc();

        // Read from main storage
        long entryLocation;
        long locationIndexStartNano = MathUtils.nowInNano();
        try {
            entryLocation = entryLocationIndex.getLocation(ledgerId, entryId);
            if (entryLocation == 0) {
                // Only a negative result while in limbo equates to unknown
                throwIfLimbo(ledgerId);

                throw new Bookie.NoEntryException(ledgerId, entryId);
            }
        } finally {
            dbLedgerStorageStats.getReadFromLocationIndexTime().addLatency(
                    MathUtils.elapsedNanos(locationIndexStartNano), TimeUnit.NANOSECONDS);
        }

        long readEntryStartNano = MathUtils.nowInNano();
        long entryLogId = logIdForOffset(entryLocation);
        if (coldEntryLogger != null && entryLogger.isArchivedEntryLog(entryLogId)) {
            LOG.info("Reading entry from coldStorage={}. entryLogId={}-{}-{}", coldLedgerBaseDir,
                    entryLogId, Long.toHexString(entryLogId), posForOffset(entryLocation));
            try {
                entry = coldEntryLogger.readEntry(ledgerId, entryId, entryLocation);
            } finally {
                dbLedgerStorageStats.getDiskCacheMissCounter().inc();
                dbLedgerStorageStats.getReadFromColdEntryLogTime().addLatency(
                        MathUtils.elapsedNanos(readEntryStartNano), TimeUnit.NANOSECONDS);
            }
            readCache.put(ledgerId, entryId, entry);

            // Try to read more entries
            long nextEntryLocation = entryLocation + 4 /* size header */ + entry.readableBytes();
            fillReadAheadCache(ledgerId, entryId + 1, nextEntryLocation);
        } else {
            LOG.info("Reading entry from diskCache={}. entryLogId={}-{}-{}", ledgerBaseDir,
                    entryLogId, Long.toHexString(entryLogId), posForOffset(entryLocation));
            try {
                entry = entryLogger.readEntry(ledgerId, entryId, entryLocation);
            } finally {
                dbLedgerStorageStats.getDiskCacheHitCounter().inc();
                dbLedgerStorageStats.getReadFromEntryLogTime().addLatency(
                        MathUtils.elapsedNanos(readEntryStartNano), TimeUnit.NANOSECONDS);
            }
        }

        return entry;
    }

    private void fillReadAheadCache(long orginalLedgerId, long firstEntryId, long firstEntryLocation) {
        long readAheadStartNano = MathUtils.nowInNano();
        int count = 0;
        long size = 0;

        try {
            long firstEntryLogId = (firstEntryLocation >> 32);
            long currentEntryLogId = firstEntryLogId;
            long currentEntryLocation = firstEntryLocation;

            while (chargeReadAheadCache(count, size) && currentEntryLogId == firstEntryLogId) {
                ByteBuf entry = coldEntryLogger.readEntry(orginalLedgerId,
                        firstEntryId, currentEntryLocation);

                try {
                    long currentEntryLedgerId = entry.getLong(0);
                    long currentEntryId = entry.getLong(8);

                    if (currentEntryLedgerId != orginalLedgerId) {
                        // Found an entry belonging to a different ledger, stopping read-ahead
                        break;
                    }

                    // Insert entry in read cache
                    readCache.put(orginalLedgerId, currentEntryId, entry);

                    count++;
                    firstEntryId++;
                    size += entry.readableBytes();

                    currentEntryLocation += 4 + entry.readableBytes();
                    currentEntryLogId = currentEntryLocation >> 32;
                } finally {
                    ReferenceCountUtil.release(entry);
                }
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Exception during read ahead for ledger: {}: e", orginalLedgerId, e);
            }
        } finally {
            dbLedgerStorageStats.getReadAheadBatchCountStats().registerSuccessfulValue(count);
            dbLedgerStorageStats.getReadAheadBatchSizeStats().registerSuccessfulValue(size);
            dbLedgerStorageStats.getReadAheadTime().addLatency(
                    MathUtils.elapsedNanos(readAheadStartNano), TimeUnit.NANOSECONDS);
        }
    }

    protected boolean chargeReadAheadCache(int currentReadAheadCount, long currentReadAheadBytes) {
        // compatible with old logic
        boolean chargeSizeCondition = currentReadAheadCount < readAheadCacheBatchSize
                && currentReadAheadBytes < maxReadAheadBytesSize;
        if (chargeSizeCondition && readAheadCacheBatchBytesSize > 0) {
            // exact limits limit the size and count for each batch
            chargeSizeCondition = currentReadAheadBytes < readAheadCacheBatchBytesSize;
        }
        return chargeSizeCondition;
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException, BookieException {
        throwIfLimbo(ledgerId);
        // Search the last entry in storage
        long locationIndexStartNano = MathUtils.nowInNano();
        long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Found last entry for ledger {} in db: {}", ledgerId, lastEntryId);
        }

        long entryLocation = entryLocationIndex.getLocation(ledgerId, lastEntryId);
        dbLedgerStorageStats.getReadFromLocationIndexTime().addLatency(
                MathUtils.elapsedNanos(locationIndexStartNano), TimeUnit.NANOSECONDS);

        long readEntryStartNano = MathUtils.nowInNano();
        long entryLogId = logIdForOffset(entryLocation);
        ByteBuf content;

        if (coldEntryLogger != null && entryLogger.isArchivedEntryLog(entryLogId)) {
            try {
                content = coldEntryLogger.readEntry(ledgerId, lastEntryId, entryLocation);
            } finally {
                dbLedgerStorageStats.getDiskCacheMissCounter().inc();
                dbLedgerStorageStats.getReadFromEntryLogTime().addLatency(
                        MathUtils.elapsedNanos(readEntryStartNano), TimeUnit.NANOSECONDS);
            }
        } else {
            try {
                content = entryLogger.readEntry(ledgerId, lastEntryId, entryLocation);
            } finally {
                dbLedgerStorageStats.getDiskCacheHitCounter().inc();
                dbLedgerStorageStats.getReadFromEntryLogTime().addLatency(
                        MathUtils.elapsedNanos(readEntryStartNano), TimeUnit.NANOSECONDS);
            }
        }
        dbLedgerStorageStats.getReadFromEntryLogTime().addLatency(
                MathUtils.elapsedNanos(readEntryStartNano), TimeUnit.NANOSECONDS);
        return content;
    }

    @VisibleForTesting
    public void logAddEntry(long ledgerId, long entryId, ByteBuf entry,
                            boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb, Object ctx)
            throws InterruptedException {
        // Retain entry until it gets written to disk
        entry.retain();

        dbLedgerStorageStats.getQueueSize().inc();

        queue.put(QueueEntry.create(
                entry, ackBeforeSync, ledgerId, entryId, cb, ctx, MathUtils.nowInNano(),
                dbLedgerStorageStats.getAddEntryStats(), callbackTime));
    }

    static class QueueEntry implements Runnable {
        ByteBuf entry;
        long ledgerId;
        long entryId;

        BookkeeperInternalCallbacks.WriteCallback cb;
        Object ctx;
        long enqueueTime;
        boolean ackBeforeSync;

        OpStatsLogger addEntryStats;
        Counter callbackTime;

        static QueueEntry create(ByteBuf entry, boolean ackBeforeSync, long ledgerId, long entryId,
                                 BookkeeperInternalCallbacks.WriteCallback cb, Object ctx, long enqueueTime,
                                 OpStatsLogger addEntryStats, Counter callbackTime) {
            QueueEntry qe = RECYCLER.get();
            qe.entry = entry;
            qe.ackBeforeSync = ackBeforeSync;
            qe.cb = cb;
            qe.ctx = ctx;
            qe.ledgerId = ledgerId;
            qe.entryId = entryId;
            qe.enqueueTime = enqueueTime;
            qe.addEntryStats = addEntryStats;
            qe.callbackTime = callbackTime;
            return qe;
        }

        private Object getCtx() {
            return ctx;
        }

        @Override
        public void run() {
            long startTime = System.nanoTime();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Acknowledge Ledger: {}, Entry: {}", ledgerId, entryId);
            }
            addEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueTime), TimeUnit.NANOSECONDS);
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
            callbackTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            recycle();
        }

        private final Recycler.Handle<QueueEntry> recyclerHandle;

        private QueueEntry(Recycler.Handle<QueueEntry> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<QueueEntry> RECYCLER = new Recycler<QueueEntry>() {
            @Override
            protected QueueEntry newObject(Recycler.Handle<QueueEntry> handle) {
                return new QueueEntry(handle);
            }
        };

        private void recycle() {
            this.entry = null;
            this.cb = null;
            this.ctx = null;
            this.callbackTime = null;
            this.addEntryStats = null;
            recyclerHandle.recycle(this);
        }
    }

    @Override
    public void run() {
        if (coldStorageBackupThread != null) {
            coldStorageBackupThread.start();
        }
        gcThread.start();
        LOG.info("Starting DirectDbLedgerStorage on {}", ledgerBaseDir);
        ThreadRegistry.register(THREAD_NAME, 0);

        RecyclableArrayList<QueueEntry> toFlush = entryListRecycler.newInstance();
        int numEntriesToFlush = 0;

        forceWriteThread.start();
        Stopwatch flushWatcher = Stopwatch.createUnstarted();
        long batchSize = 0;
        KeyValueStorage.Batch batch = entryLocationIndex.newBatch();
        try {
            boolean groupWhenTimeout = false;
            long dequeueStartTime = 0L;
            QueueEntry[] localQueueEntries = new QueueEntry[conf.getDirectStorageQueueSize()];
            int localQueueEntriesIdx = 0;
            int localQueueEntriesLen = 0;
            QueueEntry qe = null;

            while (true) {
                if (qe == null) {
                    if (dequeueStartTime != 0) {
                        dbLedgerStorageStats.getProcessTimeStats()
                                .registerSuccessfulEvent(MathUtils.elapsedNanos(dequeueStartTime), TimeUnit.NANOSECONDS);
                    }

                    // At this point the local queue will always be empty, otherwise we would have
                    // advanced to the next `qe` at the end of the loop
                    localQueueEntriesIdx = 0;
                    if (numEntriesToFlush == 0) {
                        // There are no entries pending. We can wait indefinitely until the next
                        // one is available
                        localQueueEntriesLen = queue.takeAll(localQueueEntries);
                    } else {
                        // There are already some entries pending. We must adjust
                        // the waiting time to the remaining groupWait time
                        long pollWaitTimeNanos = maxGroupWaitInNanos
                                - MathUtils.elapsedNanos(toFlush.get(0).enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTimeNanos < 0) {
                            pollWaitTimeNanos = 0;
                        }

                        localQueueEntriesLen = queue.pollAll(localQueueEntries,
                                pollWaitTimeNanos, TimeUnit.NANOSECONDS);
                    }

                    dequeueStartTime = MathUtils.nowInNano();

                    if (localQueueEntriesLen > 0) {
                        qe = localQueueEntries[localQueueEntriesIdx];
                        localQueueEntries[localQueueEntriesIdx++] = null;
                        dbLedgerStorageStats.getQueueSize().dec();
                        dbLedgerStorageStats.getQueueStats()
                                .registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime), TimeUnit.NANOSECONDS);
                    }
                } else {
                    dbLedgerStorageStats.getQueueSize().dec();
                    dbLedgerStorageStats.getQueueStats()
                            .registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime), TimeUnit.NANOSECONDS);
                }

                if (numEntriesToFlush > 0) {
                    boolean shouldFlush = false;
                    // We should issue a forceWrite if any of the three conditions below holds good
                    // 1. If the oldest pending entry has been pending for longer than the max wait time
                    if (maxGroupWaitInNanos > 0 && !groupWhenTimeout && (MathUtils
                            .elapsedNanos(toFlush.get(0).enqueueTime) > maxGroupWaitInNanos)) {
                        groupWhenTimeout = true;
                    } else if (maxGroupWaitInNanos > 0 && groupWhenTimeout
                            && (qe == null // no entry to group
                            || MathUtils.elapsedNanos(qe.enqueueTime) < maxGroupWaitInNanos)) {
                        // when group timeout, it would be better to look forward, as there might be lots of
                        // entries already timeout
                        // due to a previous slow write (writing to filesystem which impacted by force write).
                        // Group those entries in the queue
                        // a) already timeout
                        // b) limit the number of entries to group
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        dbLedgerStorageStats.getFlushMaxWaitCounter().inc();
                    } else if (qe != null
                            && ((bufferedEntriesThreshold > 0 && toFlush.size() > bufferedEntriesThreshold))) {
                        // 2. If we have buffered more than the buffWriteThreshold or bufferedEntriesThreshold
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        dbLedgerStorageStats.getFlushMaxOutstandingBytesCounter().inc();
                    } else if (qe == null && flushWhenQueueEmpty) {
                        // We should get here only if we flushWhenQueueEmpty is true else we would wait
                        // for timeout that would put is past the maxWait threshold
                        // 3. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                        // publish at a time - common case in tests.
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        dbLedgerStorageStats.getFlushEmptyQueueCounter().inc();
                    }

                    // toFlush is non-null and not empty so should be safe to access getFirst
                    if (shouldFlush) {
                        batch.flush();
                        batch.close();
                        batch = entryLocationIndex.newBatch();
                        flushWatcher.reset().start();
                        dbLedgerStorageStats.getFlushStats().registerSuccessfulEvent(
                                flushWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                        // Trace the lifetime of entries through persistence
                        if (LOG.isDebugEnabled()) {
                            for (QueueEntry e : toFlush) {
                                if (e != null) {
                                    LOG.debug("Written and queuing for flush Ledger: {}  Entry: {}",
                                            e.ledgerId, e.entryId);
                                }
                            }
                        }

                        dbLedgerStorageStats.getForceWriteBatchEntriesStats()
                                .registerSuccessfulValue(numEntriesToFlush);
                        dbLedgerStorageStats.getForceWriteBatchBytesStats()
                                .registerSuccessfulValue(batchSize);
                        if (downgradeToColdStorage.get()) {
                            forceWriteRequests.put(createForceWriteRequest(toFlush, coldEntryLogger, ledgerIndex));
                        } else {
                            forceWriteRequests.put(createForceWriteRequest(toFlush, entryLogger, ledgerIndex));
                        }

                        toFlush = entryListRecycler.newInstance();
                        numEntriesToFlush = 0;
                        batchSize = 0L;
                    }
                }

                if (!running) {
                    LOG.info("DirectDbLedgerStorage Manager is asked to shut down, quit.");
                    break;
                }

                if (qe == null) { // no more queue entry
                    continue;
                }
                long location;
                if (downgradeToColdStorage.get()) {
                    location = coldEntryLogger.addEntry(qe.ledgerId, qe.entry);
                    long entryLogId = logIdForOffset(location);
                    entryLogger.archivedEntryLog(entryLogId);
                    LOG.info("Downgrading to cold storage. {}:{}, entryLogId={}-{}-{}",
                            qe.ledgerId, qe.entryId, entryLogId, Long.toHexString(entryLogId), posForOffset(location));
                } else {
                    location = entryLogger.addEntry(qe.ledgerId, qe.entry);
                    long entryLogId = logIdForOffset(location);
                    LOG.info("Downgrading to cold storage. {}:{}, entryLogId={}-{}-{}",
                            qe.ledgerId, qe.entryId, entryLogId, Long.toHexString(entryLogId), posForOffset(location));
                }
                entryLocationIndex.addLocation(batch, qe.ledgerId, qe.entryId, location);
                long lac = qe.entry.getLong(qe.entry.readerIndex() + 16);
                updateCachedLacIfNeeded(qe.ledgerId, lac);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Entry_ADDED: LedgerId={}, entryId={}, location={}", qe.ledgerId, qe.entryId, location);
                }
                int entrySize = qe.entry.readableBytes();
                dbLedgerStorageStats.getWriteBytes().addCount(entrySize);
                batchSize += (4 + entrySize);
                ReferenceCountUtil.release(qe.entry);
                toFlush.add(qe);
                numEntriesToFlush++;
                if (localQueueEntriesIdx < localQueueEntriesLen) {
                    qe = localQueueEntries[localQueueEntriesIdx];
                    localQueueEntries[localQueueEntriesIdx++] = null;
                } else {
                    qe = null;
                }
            }
        } catch (IOException ioe) {
            LOG.error("I/O exception in DirectDbLedgerStorage thread!", ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("DirectDbLedgerStorage exits when shutting down");
        } finally {
            try {
                batch.flush();
                this.shutdown();
            } catch (Exception ioe) {
                LOG.error("Exception during shutdown", ioe);
            }
        }
        LOG.info("DirectDbLedgerStorage exited loop!");
    }


    /**
     * Token which represents the need to force a write request to the DirectStorage.
     */
    @VisibleForTesting
    public static class ForceWriteRequest {
        private RecyclableArrayList<QueueEntry> forceWriteWaiters;
        private EntryLogger entryLogger;
        private LedgerMetadataIndex ledgerIndex;
        private boolean flushed;

        public int process(ObjectHashSet<BookieRequestHandler> writeHandlers) throws IOException {
            // Notify the waiters that the force write succeeded
            for (QueueEntry qe : forceWriteWaiters) {
                if (qe != null) {
                    if (qe.getCtx() instanceof BookieRequestHandler) {
                        writeHandlers.add((BookieRequestHandler) qe.getCtx());
                    }
                    qe.run();
                }
            }
            return forceWriteWaiters.size();
        }

        private void flushFileToDisk() throws IOException {
            if (!flushed) {
                entryLogger.flush();
                ledgerIndex.flush();
                flushed = true;
            }
        }

        private final Recycler.Handle<ForceWriteRequest> recyclerHandle;

        private ForceWriteRequest(Recycler.Handle<ForceWriteRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private void recycle() {
            flushed = false;
            if (forceWriteWaiters != null) {
                forceWriteWaiters.recycle();
                forceWriteWaiters = null;
            }
            recyclerHandle.recycle(this);
        }
    }

    private ForceWriteRequest createForceWriteRequest(RecyclableArrayList<QueueEntry> forceWriteWaiters,
                                                      EntryLogger entryLogger, LedgerMetadataIndex ledgerIndex) {
        ForceWriteRequest req = forceWriteRequestsRecycler.get();
        req.entryLogger = entryLogger;
        req.ledgerIndex = ledgerIndex;
        req.forceWriteWaiters = forceWriteWaiters;
        dbLedgerStorageStats.getForceWriteQueueSize().inc();
        return req;
    }

    private static final Recycler<ForceWriteRequest> forceWriteRequestsRecycler = new Recycler<ForceWriteRequest>() {
        @Override
        protected ForceWriteRequest newObject(
                Recycler.Handle<ForceWriteRequest> handle) {
            return new ForceWriteRequest(handle);
        }
    };

    /**
     * ForceWriteThread is a background thread which makes the DirectStorage durable periodically.
     */
    private class ForceWriteThread extends BookieCriticalThread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Thread threadToNotifyOnEx;

        public ForceWriteThread(Thread threadToNotifyOnEx) {
            super("ForceWriteThread");
            this.threadToNotifyOnEx = threadToNotifyOnEx;
        }

        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");
            ThreadRegistry.register(super.getName(), 0);

            if (conf.isBusyWaitEnabled()) {
                try {
                    CpuAffinity.acquireCore();
                } catch (Exception e) {
                    LOG.warn("Unable to acquire CPU core for DirectStorage ForceWrite thread: {}", e.getMessage(), e);
                }
            }

            final ForceWriteRequest[] localRequests = new ForceWriteRequest[conf.getDirectStorageQueueSize()];
            final ObjectHashSet<BookieRequestHandler> writeHandlers = new ObjectHashSet<>();
            while (running) {
                try {
                    int numEntriesInLastForceWrite = 0;

                    int requestsCount = forceWriteRequests.takeAll(localRequests);


                    dbLedgerStorageStats.getForceWriteQueueSize().addCount(-requestsCount);

                    // Sync and mark the directDb up to the position of the last entry in the batch
                    ForceWriteRequest lastRequest = localRequests[requestsCount - 1];
                    syncLedgerToDisk(lastRequest);

                    // All the requests in the batch are now fully-synced. We can trigger sending the
                    // responses
                    for (int i = 0; i < requestsCount; i++) {
                        ForceWriteRequest req = localRequests[i];
                        numEntriesInLastForceWrite += req.process(writeHandlers);
                        req.recycle();
                    }

                    dbLedgerStorageStats.getForceWriteGroupingCountStats()
                            .registerSuccessfulValue(numEntriesInLastForceWrite);

                    writeHandlers.forEach(
                            (ObjectProcedure<? super BookieRequestHandler>)
                                    BookieRequestHandler::flushPendingResponse);
                    writeHandlers.clear();
                } catch (IOException ioe) {
                    LOG.error("I/O exception in ForceWrite thread", ioe);
                    running = false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("ForceWrite thread interrupted");
                    running = false;
                } finally {
                    cleanupExecutor.execute(() -> {
                        // There can only be one single cleanup task running because the cleanupExecutor
                        // is single-threaded
                        try {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Removing deleted ledgers from db indexes");
                            }

                            entryLocationIndex.removeOffsetFromDeletedLedgers();
                            ledgerIndex.removeDeletedLedgers();
                        } catch (Throwable t) {
                            LOG.warn("Failed to cleanup db indexes", t);
                        }
                    });
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.interrupt();
        }

        private void syncLedgerToDisk(ForceWriteRequest lastRequest) throws IOException {
            long fsyncStartTime = MathUtils.nowInNano();
            try {
                lastRequest.flushFileToDisk();
                dbLedgerStorageStats.getSyncLedgerStats().registerSuccessfulEvent(MathUtils.elapsedNanos(fsyncStartTime),
                        TimeUnit.NANOSECONDS);
            } catch (IOException ioe) {
                dbLedgerStorageStats.getSyncLedgerStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(fsyncStartTime), TimeUnit.NANOSECONDS);
                throw ioe;
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }

    /**
     * Mapping of enums to bitmaps. The bitmaps must not overlap so that we can
     * do bitwise operations on them.
     */
    private static final Map<StorageState, Integer> stateBitmaps = ImmutableMap.of(
            StorageState.NEEDS_INTEGRITY_CHECK, 0x00000001);

    @Override
    public EnumSet<StorageState> getStorageStateFlags() throws IOException {
        int flags = ledgerIndex.getStorageStateFlags();
        EnumSet<StorageState> flagsEnum = EnumSet.noneOf(StorageState.class);
        for (Map.Entry<StorageState, Integer> e : stateBitmaps.entrySet()) {
            int value = e.getValue();
            if ((flags & value) == value) {
                flagsEnum.add(e.getKey());
            }
            flags = flags & ~value;
        }
        checkState(flags == 0, "Unknown storage state flag found " + flags);
        return flagsEnum;
    }

    @Override
    public void setStorageStateFlag(StorageState flag) throws IOException {
        checkArgument(stateBitmaps.containsKey(flag), "Unsupported flag " + flag);
        int flagInt = stateBitmaps.get(flag);
        while (true) {
            int curFlags = ledgerIndex.getStorageStateFlags();
            int newFlags = curFlags | flagInt;
            if (ledgerIndex.setStorageStateFlags(curFlags, newFlags)) {
                return;
            } else {
                LOG.info("Conflict updating storage state flags {} -> {}, retrying",
                        curFlags, newFlags);
            }
        }
    }

    @Override
    public void clearStorageStateFlag(StorageState flag) throws IOException {
        checkArgument(stateBitmaps.containsKey(flag), "Unsupported flag " + flag);
        int flagInt = stateBitmaps.get(flag);
        while (true) {
            int curFlags = ledgerIndex.getStorageStateFlags();
            int newFlags = curFlags & ~flagInt;
            if (ledgerIndex.setStorageStateFlags(curFlags, newFlags)) {
                return;
            } else {
                LOG.info("Conflict updating storage state flags {} -> {}, retrying",
                        curFlags, newFlags);
            }
        }
    }


    private void cleanupStaleTransientLedgerInfo() {
        transientLedgerInfoCache.removeIf((ledgerId, ledgerInfo) -> {
            boolean isStale = ledgerInfo.isStale();
            if (isStale) {
                ledgerInfo.close();
            }

            return isStale;
        });
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException, BookieException {
        boolean isFenced = ledgerIndex.get(ledgerId).getFenced();

        if (LOG.isDebugEnabled()) {
            LOG.debug("ledger: {}, isFenced: {}.", ledgerId, isFenced);
        }

        // Only a negative result while in limbo equates to unknown
        if (!isFenced) {
            throwIfLimbo(ledgerId);
        }

        return isFenced;
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public long addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
                                 LedgerCache.PageEntriesIterable pages) throws Exception {
        DbLedgerStorageDataFormats.LedgerData ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(isFenced)
                .setMasterKey(ByteString.copyFrom(masterKey)).build();
        ledgerIndex.set(ledgerId, ledgerData);
        MutableLong numberOfEntries = new MutableLong();

        // Iterate over all the entries pages
        KeyValueStorage.Batch batch = entryLocationIndex.newBatch();
        for (LedgerCache.PageEntries page : pages) {
            try (LedgerEntryPage lep = page.getLEP()) {
                lep.getEntries((entryId, location) -> {
                    entryLocationIndex.addLocation(batch, ledgerId, entryId, location);
                    numberOfEntries.increment();
                    return true;
                });
            }
        }

        ledgerIndex.flush();
        batch.flush();
        batch.close();

        return numberOfEntries.longValue();
    }

    public EntryLocationIndex getEntryLocationIndex() {
        return entryLocationIndex;
    }
}
