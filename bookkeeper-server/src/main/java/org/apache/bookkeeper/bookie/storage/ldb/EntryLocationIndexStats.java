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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;

import java.util.function.Supplier;
import lombok.Getter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * A umbrella class for ledger metadata index stats.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "Entry location index stats"
)
@Getter
class EntryLocationIndexStats {

    private static final String ENTRIES_COUNT = "entries-count";
    private static final String LOOKUP_ENTRY_LOCATION = "lookup-entry-location";
    private static final String BLOCK_CACHE_CAPACITY = "block-cache-capacity";
    private static final String BLOCK_CACHE_USAGE = "block-cache-usage";

    @StatsDoc(
        name = ENTRIES_COUNT,
        help = "Current number of entries"
    )
    private final Gauge<Long> entriesCountGauge;


    @StatsDoc(
            name = BLOCK_CACHE_CAPACITY,
            help = "Rocksdb block cache capacity."
    )
    private final Gauge<Long> blockCacheCapacityGauge;

    @StatsDoc(
            name = BLOCK_CACHE_USAGE,
            help = "the memory size for the entries residing in block cache"
    )
    private final Gauge<Long> blockCacheUsageGauge;
    @StatsDoc(
            name = LOOKUP_ENTRY_LOCATION,
            help = "operation stats of looking up entry location"
    )
    private final OpStatsLogger lookupEntryLocationStats;

    EntryLocationIndexStats(StatsLogger statsLogger,
                            Supplier<Long> entriesCountSupplier,
                            Supplier<Long> blockCacheCapacitySupplier,
                            Supplier<Long> blockCacheUsageSupplier) {
        entriesCountGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return entriesCountSupplier.get();
            }
        };
        statsLogger.registerGauge(ENTRIES_COUNT, entriesCountGauge);
        blockCacheCapacityGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return blockCacheCapacitySupplier.get();
            }
        };
        statsLogger.registerGauge(BLOCK_CACHE_CAPACITY, blockCacheCapacityGauge);
        blockCacheUsageGauge = new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return blockCacheUsageSupplier.get();
            }
        };
        statsLogger.registerGauge(BLOCK_CACHE_USAGE, blockCacheUsageGauge);
        lookupEntryLocationStats = statsLogger.getOpStatsLogger(LOOKUP_ENTRY_LOCATION);
    }

}
