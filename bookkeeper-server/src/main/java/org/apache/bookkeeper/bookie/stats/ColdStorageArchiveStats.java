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
    final StatsLogger statsLogger;
    @StatsDoc(
        name = RECLAIMED_DISK_CACHE_SPACE_BYTES,
        help = "Number of disk cache space bytes reclaimed via archiving old entry log files"
    )
    private final Counter reclaimedDiskCacheSpaceViaArchive;

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
        this.archiveThreadRuntime = statsLogger.getOpStatsLogger(ARCHIVE_THREAD_RUNTIME);
        this.extractionRunTime = statsLogger.getOpStatsLogger(EXTRACT_METADATA_RUNTIME);

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
