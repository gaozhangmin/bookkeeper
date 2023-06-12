package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.logIdFromLocation;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.makeEntry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class DbLedgerStorageDiskCacheTest {
    File coldTmpDir;
    protected DbLedgerStorage storage;
    protected File tmpDir;
    protected LedgerDirsManager ledgerDirsManager;
    protected ServerConfiguration conf;

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        coldTmpDir = File.createTempFile("bkTest", ".colddir");
        coldTmpDir.delete();
        coldTmpDir.mkdir();
        File coldDir = BookieImpl.getCurrentDirectory(coldTmpDir);
        BookieImpl.checkDirectoryStructure(coldDir);

        int gcWaitTime = 1000;
        conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setColdLedgerDirName(new String[] { coldTmpDir.toString() });
        conf.setWarmStorageRetentionTime(1000);
        conf.setEntryLogSizeLimit(20000);
        BookieImpl bookie = new TestBookieImpl(conf);

        ledgerDirsManager = bookie.getLedgerDirsManager();
        storage = (DbLedgerStorage) bookie.getLedgerStorage();

        storage.getLedgerStorageList().forEach(singleDirectoryDbLedgerStorage -> {
            assertTrue(singleDirectoryDbLedgerStorage.getEntryLogger() instanceof DefaultEntryLogger);
        });
    }

    @After
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }

    @Test
    public void testRewritingEntries() throws Exception {
        // Simulate bookie compaction
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        EntryLogger coldEntryLogger = singleDirStorage.getColdEntryLogger();

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
        assertTrue(entryLogger.logExists(logId3));
        assertTrue(entryLogger.logExists(logId2));
        assertTrue(entryLogger.logExists(logId1));
        assertTrue(entryLogger.logExists(logId4));

        assertTrue(storage.ledgerExists(1L));
        assertTrue(storage.ledgerExists(2L));
        assertTrue(storage.ledgerExists(3L));


        singleDirStorage.getColdStorageBackupThread().start();

        assertTrue(storage.ledgerExists(1L));
        assertTrue(storage.ledgerExists(2L));
        assertTrue(storage.ledgerExists(3L));

        Thread.sleep(5000);


        // after archiving, the entry logger should be empty
        assertTrue(entryLogger.getFlushedLogIds().isEmpty());
        assertFalse(entryLogger.logExists(logId1));
        assertFalse(entryLogger.logExists(logId2));
        assertFalse(entryLogger.logExists(logId3));

        // cold entry logger should have all the logs
        assertTrue(coldEntryLogger.logExists(logId1));
        assertTrue(coldEntryLogger.logExists(logId2));
        assertTrue(coldEntryLogger.logExists(logId3));

        // ledgers still exist
        assertTrue(storage.ledgerExists(1L));
        assertTrue(storage.ledgerExists(2L));
        assertTrue(storage.ledgerExists(3L));


        ByteBuf response = coldEntryLogger.readEntry(1L, 1L, loc1);
        assertEquals(makeEntry(1L, 1L, 5000), response);

        response = coldEntryLogger.readEntry(2L, 1L, loc2);
        assertEquals(makeEntry(2L, 1L, 5000), response);

        response = coldEntryLogger.readEntry(2L, 2L, loc3);
        assertEquals(makeEntry(2L, 2L, 15000), response);

        response = coldEntryLogger.readEntry(2L, 3L, loc4);
        assertEquals(makeEntry(2L, 3L, 15000), response);

        response = coldEntryLogger.readEntry(3L, 1L, loc5);
        assertEquals(makeEntry(3L, 1L, 1000), response);
    }
}
