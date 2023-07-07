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

package org.apache.bookkeeper.bookie.storage.ldb;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;

import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

import java.util.function.Supplier;


/**
 * A umbrella class for journal related stats.
 */
@StatsDoc(
        name = BOOKIE_SCOPE,
        category = CATEGORY_SERVER,
        help = "DbLedgerStorage related stats"
)
@Getter
public class DirectDBLedgerStorageStats {
    private static final String READ_ENTRY_LOCATIONS_INDEX_TIME = "read-locations-index-time";
    private static final String READ_ENTRYLOG_TIME = "read-entrylog-time";
    private static final String READ_COLD_ENTRYLOG_TIME = "read-cold-entrylog-time";
    private static final String ADD_ENTRY = "add-entry";
    private static final String READ_ENTRY = "read-entry";
    private static final String BOOKIE_DIRECT_STORAGE_SYNC = "BOOKIE_DIRECT_STORAGE_SYNC";
    private static final String BOOKIE_DIRECT_STORAGE_FORCE = "BOOKIE_DIRECT_STORAGE_FORCE";
    private static final String BOOKIE_DIRECT_STORAGE_FLUSH_LATENCY = "BOOKIE_DIRECT_STORAGE_FLUSH_LATENCY";
    private static final String BOOKIE_DIRECT_STORAGE_PROCESS_TIME_LATENCY = "BOOKIE_DIRECT_STORAGE_PROCESS_TIME_LATENCY";
    private static final String BOOKIE_DIRECT_STORAGE_QUEUE_LATENCY = "BOOKIE_DIRECT_STORAGE_QUEUE_LATENCY";
    private static final String BOOKIE_DIRECT_STORAGE_FORCE_WRITE_GROUPING_COUNT = "BOOKIE_DIRECT_STORAGE_FORCE_WRITE_GROUPING_TOTAL";
    private static final String BOOKIE_DIRECT_STORAGE_WRITE_BATCH_ENTRIES = "BOOKIE_DIRECT_STORAGE_WRITE_BATCH_ENTRIES";
    private static final String BOOKIE_DIRECT_STORAGE_FORCE_WRITE_BATCH_BYTES = "BOOKIE_DIRECT_STORAGE_FORCE_WRITE_BATCH_BYTES";
    private static final String BOOKIE_DIRECT_STORAGE_QUEUE_SIZE = "BOOKIE_DIRECT_STORAGE_QUEUE_SIZE";
    private static final String BOOKIE_DIRECT_STORAGE_WRITE_QUEUE_SIZE = "BOOKIE_DIRECT_STORAGE_FORCE_WRITE_QUEUE_SIZE";
    private static final String BOOKIE_DIRECT_STORAGE_NUM_FLUSH_MAX_WAIT = "BOOKIE_DIRECT_STORAGE_NUM_FLUSH_MAX_WAIT";
    private static final String BOOKIE_DIRECT_STORAGE_FLUSH_MAX_OUTSTANDING_BYTES = "BOOKIE_DIRECT_STORAGE_NUM_FLUSH_MAX_OUTSTANDING_BYTES";
    private static final String BOOKIE_DIRECT_STORAGE_NUM_FLUSH_EMPTY_QUEUE = "BOOKIE_DIRECT_STORAGE_NUM_FLUSH_EMPTY_QUEUE";
    private static final String BOOKIE_DIRECT_STORAGE_WRITE_BYTES = "BOOKIE_DIRECT_STORAGE_WRITE_BYTES";
    private static final String DISK_CACHE_HITS = "disk-cache-hits";
    private static final String DISK_CACHE_MISSES = "disk-cache-misses";
    private static final String READ_CACHE_SIZE = "read-cache-size";
    private static final String READ_CACHE_COUNT = "read-cache-count";
    private static final String READ_CACHE_HITS = "read-cache-hits";
    private static final String READ_CACHE_MISSES = "read-cache-misses";
    private static final String READAHEAD_BATCH_COUNT = "readahead-batch-count";
    private static final String READAHEAD_BATCH_SIZE = "readahead-batch-size";
    private static final String READAHEAD_TIME = "readahead-time";

    @StatsDoc(
            name = ADD_ENTRY,
            help = "operation stats of adding entries to db ledger storage",
            parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger addEntryStats;
    @StatsDoc(
            name = READ_ENTRY,
            help = "operation stats of reading entries from db ledger storage",
            parent = BOOKIE_READ_ENTRY
    )
    private final OpStatsLogger readEntryStats;
    @StatsDoc(
            name = READ_ENTRY_LOCATIONS_INDEX_TIME,
            help = "time spent reading entries from the locations index of the db ledger storage engine",
            parent = READ_ENTRY
    )
    private final Counter readFromLocationIndexTime;
    @StatsDoc(
            name = READ_ENTRYLOG_TIME,
            help = "time spent reading entries from the entry log files of the db ledger storage engine",
            parent = READ_ENTRY
    )
    private final Counter readFromEntryLogTime;
    @StatsDoc(
            name = READ_COLD_ENTRYLOG_TIME,
            help = "time spent reading entries from the cold entry log files of the db ledger storage engine",
            parent = READ_ENTRY
    )
    private final Counter readFromColdEntryLogTime;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_FORCE,
        help = "operation stats of recording forceLedger requests in the journal",
        parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger forceLedgerStats;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_SYNC,
        help = "operation stats of syncing data to journal disks",
        parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger SyncLedgerStats;

    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_FLUSH_LATENCY,
        help = "operation stats of flushing data from memory to filesystem (but not yet fsyncing to disks)",
        parent = BOOKIE_DIRECT_STORAGE_PROCESS_TIME_LATENCY
    )
    private final OpStatsLogger flushStats;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_PROCESS_TIME_LATENCY,
        help = "operation stats of processing requests in a ledger storage (from dequeue an item to finish processing" +
                " it)",
        parent = BOOKIE_ADD_ENTRY,
        happensAfter = BOOKIE_DIRECT_STORAGE_QUEUE_LATENCY
    )
    private final OpStatsLogger processTimeStats;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_QUEUE_LATENCY,
        help = "operation stats of enqueuing requests to a journal",
        parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger queueStats;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_FORCE_WRITE_GROUPING_COUNT,
        help = "The distribution of number of force write requests grouped in a force write"
    )
    private final OpStatsLogger forceWriteGroupingCountStats;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_WRITE_BATCH_ENTRIES,
        help = "The distribution of number of entries grouped together into a force write request"
    )
    private final OpStatsLogger forceWriteBatchEntriesStats;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_FORCE_WRITE_BATCH_BYTES,
        help = "The distribution of number of bytes grouped together into a force write request"
    )
    private final OpStatsLogger forceWriteBatchBytesStats;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_QUEUE_SIZE,
        help = "The direct storage queue size"
    )
    private final Counter queueSize;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_WRITE_QUEUE_SIZE,
        help = "The force write queue size"
    )
    private final Counter forceWriteQueueSize;

    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_NUM_FLUSH_MAX_WAIT,
        help = "The number of journal flushes triggered by MAX_WAIT time"
    )
    private final Counter flushMaxWaitCounter;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_FLUSH_MAX_OUTSTANDING_BYTES,
        help = "The number of journal flushes triggered by MAX_OUTSTANDING_BYTES"
    )
    private final Counter flushMaxOutstandingBytesCounter;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_NUM_FLUSH_EMPTY_QUEUE,
        help = "The number of journal flushes triggered when journal queue becomes empty"
    )
    private final Counter flushEmptyQueueCounter;
    @StatsDoc(
        name = BOOKIE_DIRECT_STORAGE_WRITE_BYTES,
        help = "The number of bytes appended to the journal"
    )
    private final Counter writeBytes;
    @StatsDoc(
            name = DISK_CACHE_HITS,
            help = "number of disk cache hits (on reads)",
            parent = READ_ENTRY
    )
    private final Counter diskCacheHitCounter;
    @StatsDoc(
            name = DISK_CACHE_MISSES,
            help = "number of disk cache misses (on reads)",
            parent = READ_ENTRY
    )
    private final Counter diskCacheMissCounter;
    @StatsDoc(
            name = READ_CACHE_SIZE,
            help = "Current number of bytes in read cache"
    )
    private final Gauge<Long> readCacheSizeGauge;
    @StatsDoc(
            name = READ_CACHE_COUNT,
            help = "Current number of entries in read cache"
    )
    private final Gauge<Long> readCacheCountGauge;
    @StatsDoc(
            name = READ_CACHE_HITS,
            help = "number of read cache hits",
            parent = READ_ENTRY
    )
    private final Counter readCacheHitCounter;
    @StatsDoc(
            name = READ_CACHE_MISSES,
            help = "number of read cache misses",
            parent = READ_ENTRY
    )
    private final Counter readCacheMissCounter;
    @StatsDoc(
            name = READAHEAD_BATCH_COUNT,
            help = "the distribution of num of entries to read in one readahead batch"
    )
    private final OpStatsLogger readAheadBatchCountStats;
    @StatsDoc(
            name = READAHEAD_BATCH_SIZE,
            help = "the distribution of num of bytes to read in one readahead batch"
    )
    private final OpStatsLogger readAheadBatchSizeStats;
    @StatsDoc(
            name = READAHEAD_TIME,
            help = "Time spent on readahead operations"
    )
    private final Counter readAheadTime;

    public DirectDBLedgerStorageStats(StatsLogger statsLogger,
                                      Supplier<Long> readCacheSizeSupplier,
                                      Supplier<Long> readCacheCountSupplier) {
        addEntryStats = statsLogger.getThreadScopedOpStatsLogger(ADD_ENTRY);
        readEntryStats = statsLogger.getThreadScopedOpStatsLogger(READ_ENTRY);
        readFromLocationIndexTime = statsLogger.getThreadScopedCounter(READ_ENTRY_LOCATIONS_INDEX_TIME);
        readFromEntryLogTime = statsLogger.getThreadScopedCounter(READ_ENTRYLOG_TIME);
        readFromColdEntryLogTime = statsLogger.getThreadScopedCounter(READ_COLD_ENTRYLOG_TIME);

        forceLedgerStats = statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_FORCE);
        SyncLedgerStats = statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_SYNC);

        flushStats = statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_FLUSH_LATENCY);
        queueStats = statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_QUEUE_LATENCY);
        processTimeStats = statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_PROCESS_TIME_LATENCY);
        forceWriteGroupingCountStats =
                statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_FORCE_WRITE_GROUPING_COUNT);
        forceWriteBatchEntriesStats =
                statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_WRITE_BATCH_ENTRIES);
        forceWriteBatchBytesStats = statsLogger.getOpStatsLogger(BOOKIE_DIRECT_STORAGE_FORCE_WRITE_BATCH_BYTES);
        queueSize = statsLogger.getCounter(BOOKIE_DIRECT_STORAGE_QUEUE_SIZE);
        forceWriteQueueSize = statsLogger.getCounter(BOOKIE_DIRECT_STORAGE_WRITE_QUEUE_SIZE);
        flushMaxWaitCounter = statsLogger.getCounter(BOOKIE_DIRECT_STORAGE_NUM_FLUSH_MAX_WAIT);
        flushMaxOutstandingBytesCounter =
                statsLogger.getCounter(BOOKIE_DIRECT_STORAGE_FLUSH_MAX_OUTSTANDING_BYTES);
        flushEmptyQueueCounter = statsLogger.getCounter(BOOKIE_DIRECT_STORAGE_NUM_FLUSH_EMPTY_QUEUE);
        writeBytes = statsLogger.getCounter(BOOKIE_DIRECT_STORAGE_WRITE_BYTES);
        diskCacheHitCounter = statsLogger.getCounter(DISK_CACHE_HITS);
        diskCacheMissCounter = statsLogger.getCounter(DISK_CACHE_MISSES);
        readCacheHitCounter = statsLogger.getCounter(READ_CACHE_HITS);
        readCacheMissCounter = statsLogger.getCounter(READ_CACHE_MISSES);
        readAheadBatchCountStats = statsLogger.getOpStatsLogger(READAHEAD_BATCH_COUNT);
        readAheadBatchSizeStats = statsLogger.getOpStatsLogger(READAHEAD_BATCH_SIZE);
        readAheadTime = statsLogger.getThreadScopedCounter(READAHEAD_TIME);

        readCacheSizeGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readCacheSizeSupplier.get();
            }
        };
        statsLogger.registerGauge(READ_CACHE_SIZE, readCacheSizeGauge);
        readCacheCountGauge = new Gauge<Long>() {

            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readCacheCountSupplier.get();
            }
        };
        statsLogger.registerGauge(READ_CACHE_COUNT, readCacheCountGauge);

    }

}
