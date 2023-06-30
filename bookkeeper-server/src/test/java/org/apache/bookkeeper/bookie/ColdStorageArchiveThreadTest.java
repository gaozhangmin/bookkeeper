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

import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MockLedgerManager;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

/**
 * Unit test for {@link GarbageCollectorThread}.
 */
@SuppressWarnings("deprecation")
public class ColdStorageArchiveThreadTest {
    private final TmpDirs tmpDirs = new TmpDirs();
    private ServerConfiguration conf = spy(new ServerConfiguration().setAllowLoopback(true));

    @Before
    public void setUp() throws Exception {
        conf.setAllowLoopback(true);
        conf.setWarmStorageRetentionTime(1000);
        openMocks(this);
    }

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }

    @Test
    public void testExtractMetaFromEntryLogsLegacy() throws Exception {
        File ledgerDir = tmpDirs.createNew("testExtractMeta", "ledgers");
        File coldLedgerDir = tmpDirs.createNew("testColdExtractMeta", "ledgers");
        testExtractMetaFromEntryLogs(
                newLegacyEntryLogger(20000, ledgerDir),
                newLegacyEntryLogger(20000, coldLedgerDir), ledgerDir, coldLedgerDir);
    }

    private void testExtractMetaFromEntryLogs(EntryLogger entryLogger,
                                              EntryLogger coldEntryLogger,
                                              File ledgerDir, File coldLedgerDir)
            throws Exception {

        MockLedgerStorage storage = new MockLedgerStorage();
        MockLedgerManager lm = new MockLedgerManager();

        ColdStorageArchiveThread gcThread = new ColdStorageArchiveThread(
                TestBKConfiguration.newServerConfiguration(), lm,
                newDirsManager(ledgerDir),
                newDirsManager(coldLedgerDir),
                storage, entryLogger, coldEntryLogger,
                NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        // Add entries.
        // Ledger 1 is on first entry log
        // Ledger 2 spans first, second and third entry log
        // Ledger 3 is on the third entry log (which is still active when extract meta)
        long loc1 = entryLogger.addEntry(1L, makeEntry(1L, 1L, 5000));
        long loc2 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 5000));
        assertThat(logIdFromLocation(loc2), equalTo(logIdFromLocation(loc1)));
        long loc3 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 15000));
        assertThat(logIdFromLocation(loc3), greaterThan(logIdFromLocation(loc2)));
        long loc4 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 15000));
        assertThat(logIdFromLocation(loc4), greaterThan(logIdFromLocation(loc3)));
        long loc5 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 1000));
        assertThat(logIdFromLocation(loc5), equalTo(logIdFromLocation(loc4)));

        long logId1 = logIdFromLocation(loc2);
        long logId2 = logIdFromLocation(loc3);
        long logId3 = logIdFromLocation(loc5);
        entryLogger.flush();

        storage.setMasterKey(1L, new byte[0]);
        storage.setMasterKey(2L, new byte[0]);
        storage.setMasterKey(3L, new byte[0]);

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2));
        assertTrue(entryLogger.logExists(logId3));

        // all ledgers exist, nothing should disappear
        final EntryLogMetadataMap entryLogMetaMap = gcThread.getEntryLogMetaMap();
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId2));
        assertTrue(entryLogger.logExists(logId3));

        // log 2 is 100% ledger 2, so it should disappear if ledger 2 is deleted
        entryLogMetaMap.clear();
        storage.deleteLedger(2L);
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogger.logExists(logId3));

        // delete all ledgers, all logs except the current should be deleted
        entryLogMetaMap.clear();
        storage.deleteLedger(1L);
        storage.deleteLedger(3L);
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), empty());
        assertTrue(entryLogMetaMap.isEmpty());
        assertTrue(entryLogger.logExists(logId3));

        // add enough entries to roll log, log 3 can not be GC'd
        long loc6 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 25000));
        assertThat(logIdFromLocation(loc6), greaterThan(logIdFromLocation(loc5)));
        entryLogger.flush();
        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId3));

        entryLogMetaMap.clear();
        gcThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), empty());
        assertTrue(entryLogMetaMap.isEmpty());
        assertFalse(entryLogger.logExists(logId3));
    }

    @Test
    public void testCompactionWithFileSizeCheck() throws Exception {
        File ledgerDir = tmpDirs.createNew("testColdArchive", "ledgers");
        File coldLedgerDir = tmpDirs.createNew("testColdArchiveCold", "ledgers");
        EntryLogger entryLogger = newLegacyEntryLogger(20000, ledgerDir);
        EntryLogger coldEntryLogger = newLegacyEntryLogger(20000, coldLedgerDir);

        MockLedgerStorage storage = new MockLedgerStorage();
        MockLedgerManager lm = new MockLedgerManager();

        ColdStorageArchiveThread coldArchiveThread = new ColdStorageArchiveThread(
            TestBKConfiguration.newServerConfiguration().setUseTargetEntryLogSizeForGc(true)
                    .setWarmStorageRetentionTime(1000), lm,
            newDirsManager(ledgerDir),
            newDirsManager(coldLedgerDir),
            storage, entryLogger, coldEntryLogger, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        // Add entries.
        // Ledger 1 is on first entry log
        // Ledger 2 spans first, second and third entry log
        // Ledger 3 is on the third entry log (which is still active when extract meta)
        long loc1 = entryLogger.addEntry(1L, makeEntry(1L, 1L, 5000));
        long loc2 = entryLogger.addEntry(2L, makeEntry(2L, 1L, 5000));
        assertThat(logIdFromLocation(loc2), equalTo(logIdFromLocation(loc1)));
        long loc3 = entryLogger.addEntry(2L, makeEntry(2L, 2L, 15000));
        assertThat(logIdFromLocation(loc3), greaterThan(logIdFromLocation(loc2)));
        long loc4 = entryLogger.addEntry(2L, makeEntry(2L, 3L, 15000));
        assertThat(logIdFromLocation(loc4), greaterThan(logIdFromLocation(loc3)));
        long loc5 = entryLogger.addEntry(3L, makeEntry(3L, 1L, 1000));
        assertThat(logIdFromLocation(loc5), equalTo(logIdFromLocation(loc4)));
        long loc6 = entryLogger.addEntry(3L, makeEntry(3L, 2L, 5000));

        long logId1 = logIdFromLocation(loc2);
        long logId2 = logIdFromLocation(loc3);
        long logId3 = logIdFromLocation(loc5);
        long logId4 = logIdFromLocation(loc6);
        entryLogger.flush();

        storage.setMasterKey(1L, new byte[0]);
        storage.setMasterKey(2L, new byte[0]);
        storage.setMasterKey(3L, new byte[0]);

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2, logId3));
        assertTrue(entryLogger.logExists(logId1));
        assertTrue(entryLogger.logExists(logId2));
        assertTrue(entryLogger.logExists(logId3));
        assertTrue(entryLogger.logExists(logId4));

        // all ledgers exist, nothing should disappear
        final EntryLogMetadataMap entryLogMetaMap = coldArchiveThread.getEntryLogMetaMap();
        coldArchiveThread.extractMetaFromEntryLogs();

        assertThat(entryLogger.getFlushedLogIds(), containsInAnyOrder(logId1, logId2, logId3));
        assertTrue(entryLogMetaMap.containsKey(logId1));
        assertTrue(entryLogMetaMap.containsKey(logId2));
        assertTrue(entryLogger.logExists(logId3));

        Thread.sleep(2000);

        coldArchiveThread.runWithFlags();

        assertTrue(entryLogger.getFlushedLogIds().isEmpty());
        assertFalse(entryLogger.logExists(logId1));
        assertFalse(entryLogger.logExists(logId2));
        assertFalse(entryLogger.logExists(logId3));

        assertTrue(coldEntryLogger.logExists(logId1));
        assertTrue(coldEntryLogger.logExists(logId2));
        assertTrue(coldEntryLogger.logExists(logId3));


        assertTrue(storage.ledgerExists(1L));
        assertTrue(storage.ledgerExists(2L));
        assertTrue(storage.ledgerExists(3L));
    }
}
