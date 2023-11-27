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

package org.apache.bookkeeper.bookie.stats;

import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

import java.util.function.Supplier;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

/**
 * A umbrella class for gc stats.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "Cold storage archive related stats"
)
@Getter
public class ColdStorageArchiveStats {
    private static final String ARCHIVE_ENTRY = "archive-entry";
    private static final String FLUSH_ARCHIVED_ENTRYLOG = "flush-archived-entrylog";
    private static final String FLUSH_ARCHIVED_LOCATIONS_INDEX = "flush-archived-locations-index";
    private static final String FLUSH_ARCHIVE = "flush-archive";
    private static final String FLUSH_ARCHIVE_SIZE = "flush-archive-size";


    final StatsLogger statsLogger;
    @StatsDoc(
        name = RECLAIMED_DISK_CACHE_SPACE_BYTES,
        help = "Number of disk cache space bytes reclaimed via archiving old entry log files"
    )
    private final Counter reclaimedDiskCacheSpaceViaArchive;
    @StatsDoc(
            name = ARCHIVE_ENTRY,
            help = "operation stats of archiv entries",
            parent = BOOKIE_ADD_ENTRY
    )
    private final OpStatsLogger archiveEntryStats;
    @StatsDoc(
            name = FLUSH_ARCHIVED_ENTRYLOG,
            help = "operation stats of flushing to the current entry log file"
    )
    private final OpStatsLogger flushEntryLogStats;
    @StatsDoc(
            name = FLUSH_ARCHIVED_LOCATIONS_INDEX,
            help = "operation stats of flushing to the locations index"
    )
    private final OpStatsLogger flushLocationIndexStats;
    @StatsDoc(
            name = FLUSH_ARCHIVE,
            help = "operation stats of flushing write cache to entry log files"
    )
    private final OpStatsLogger flushStats;
    @StatsDoc(
            name = FLUSH_ARCHIVE_SIZE,
            help = "the distribution of number of bytes flushed from write cache to entry log files"
    )
    private final OpStatsLogger flushSizeStats;

    @StatsDoc(
            name = ARCHIVED_ENTRY_LOG_SIZE_BYTES,
            help = "Size of data via archiving old entry log files"
    )
    private final Counter archivedEntryLogSize;

    @StatsDoc(
        name = DISK_CACHE_ACTIVE_ENTRY_LOG_COUNT,
        help = "Current number of active entry log files in disks cache"
    )
    private final Gauge<Integer> activeEntryLogCountGauge;
    @StatsDoc(
        name = DISK_CACHE_ACTIVE_ENTRY_LOG_SPACE_BYTES,
        help = "Current number of active entry log space bytes in disks cache"
    )
    private final Gauge<Long> activeEntryLogSpaceBytesGauge;
    @StatsDoc(
            name = ARCHIVE_THREAD_RUNTIME,
            help = "Operation stats of archive thread runtime for entry log files"
    )
    private final OpStatsLogger archiveThreadRuntime;

    @StatsDoc(
            name = EXTRACT_METADATA_RUNTIME,
            help = "Operation stats of entry log files metadata extraction runtime"
    )
    private final OpStatsLogger extractionRunTime;

    public ColdStorageArchiveStats(StatsLogger statsLogger,
                                   Supplier<Integer> activeEntryLogCountSupplier,
                                   Supplier<Long> activeEntryLogSpaceBytesSupplier) {
        this.statsLogger = statsLogger;
        this.reclaimedDiskCacheSpaceViaArchive = statsLogger.getCounter(RECLAIMED_DISK_CACHE_SPACE_BYTES);
        this.archiveEntryStats = statsLogger.getThreadScopedOpStatsLogger(ARCHIVE_ENTRY);
        this.archivedEntryLogSize = statsLogger.getCounter(ARCHIVED_ENTRY_LOG_SIZE_BYTES);
        this.archiveThreadRuntime = statsLogger.getOpStatsLogger(ARCHIVE_THREAD_RUNTIME);
        this.extractionRunTime = statsLogger.getOpStatsLogger(EXTRACT_METADATA_RUNTIME);
        this.flushEntryLogStats = statsLogger.getOpStatsLogger(FLUSH_ARCHIVED_ENTRYLOG);
        this.flushLocationIndexStats = statsLogger.getOpStatsLogger(FLUSH_ARCHIVED_LOCATIONS_INDEX);
        this.flushStats = statsLogger.getOpStatsLogger(FLUSH_ARCHIVE);
        this.flushSizeStats = statsLogger.getOpStatsLogger(FLUSH_ARCHIVE_SIZE);


        this.activeEntryLogCountGauge = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return activeEntryLogCountSupplier.get();
            }
        };
        statsLogger.registerGauge(DISK_CACHE_ACTIVE_ENTRY_LOG_COUNT, activeEntryLogCountGauge);
        this.activeEntryLogSpaceBytesGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return activeEntryLogSpaceBytesSupplier.get();
            }
        };
        statsLogger.registerGauge(DISK_CACHE_ACTIVE_ENTRY_LOG_SPACE_BYTES, activeEntryLogSpaceBytesGauge);
    }

}
