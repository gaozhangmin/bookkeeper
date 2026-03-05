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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to verify Entry Log Manager shutdown behavior.
 * Specifically tests that appendLedgersMap() is called during shutdown.
 */
public class EntryLogManagerShutdownTest {

    private static final Logger LOG = LoggerFactory.getLogger(EntryLogManagerShutdownTest.class);

    private ServerConfiguration conf;
    private LedgerDirsManager dirsMgr;
    private File tmpDir;
    private File curDir;
    private List<File> tempDirs = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        tmpDir = createTempDir("bkTest", ".dir");
        tempDirs.add(tmpDir);

        curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setFlushIntervalInBytes(0);

        dirsMgr = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
    }

    @After
    public void tearDown() throws Exception {
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    private File createTempDir(String prefix, String suffix) throws IOException {
        File dir = File.createTempFile(prefix, suffix);
        dir.delete();
        dir.mkdir();
        return dir;
    }

    private ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    /**
     * Test EntryLogManagerForSingleEntryLog shutdown behavior.
     * Verifies that appendLedgersMap() is called during close().
     */
    @Test
    public void testSingleEntryLogManagerShutdown() throws Exception {
        conf.setEntryLogPerLedgerEnabled(false);

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, dirsMgr);

        // Add some entries to create a ledger map
        long ledgerId1 = 1L;
        long ledgerId2 = 2L;

        entryLogger.addEntry(ledgerId1, generateEntry(ledgerId1, 0).nioBuffer());
        entryLogger.addEntry(ledgerId2, generateEntry(ledgerId2, 0).nioBuffer());
        entryLogger.addEntry(ledgerId1, generateEntry(ledgerId1, 1).nioBuffer());

        // Flush to ensure data is written
        entryLogger.flush();

        // Get current log ID before shutdown
        EntryLogManager entryLogManager = entryLogger.getEntryLogManager();
        assertTrue("Entry log manager should be EntryLogManagerForSingleEntryLog",
                entryLogManager instanceof EntryLogManagerForSingleEntryLog);

        long currentLogId = ((EntryLogManagerForSingleEntryLog) entryLogManager).getCurrentLogId();

        // Shutdown the entry logger - this should call appendLedgersMap()
        entryLogger.close();

        // Verify that the log file has ledger metadata by trying to read it
        File logFile = new File(curDir, currentLogId + ".log");
        assertTrue("Log file should exist after shutdown", logFile.exists());

        // Create new entry logger to read the metadata
        DefaultEntryLogger newEntryLogger = new DefaultEntryLogger(conf, dirsMgr);

        // Try to extract metadata - this will succeed only if appendLedgersMap was called
        try {
            EntryLogMetadata metadata = newEntryLogger.getEntryLogMetadata(currentLogId);
            assertNotNull("Metadata should not be null", metadata);
            assertTrue("Metadata should contain ledger " + ledgerId1,
                      metadata.containsLedger(ledgerId1));
            assertTrue("Metadata should contain ledger " + ledgerId2,
                      metadata.containsLedger(ledgerId2));
            LOG.info("SUCCESS: Entry log {} contains proper ledger metadata after shutdown", currentLogId);
        } catch (IOException e) {
            LOG.error("FAILURE: Could not read ledger metadata from entry log {}", currentLogId, e);
            throw new AssertionError("Entry log should contain ledger metadata after proper shutdown", e);
        } finally {
            newEntryLogger.close();
        }
    }

    /**
     * Test EntryLogManagerForEntryLogPerLedger shutdown behavior.
     * Verifies that appendLedgersMap() is called for all active logs during close().
     */
    @Test
    public void testPerLedgerEntryLogManagerShutdown() throws Exception {
        conf.setEntryLogPerLedgerEnabled(true);

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, dirsMgr);

        // Add entries for different ledgers to create multiple logs
        long ledgerId1 = 1L;
        long ledgerId2 = 2L;
        long ledgerId3 = 3L;

        entryLogger.addEntry(ledgerId1, generateEntry(ledgerId1, 0).nioBuffer());
        entryLogger.addEntry(ledgerId2, generateEntry(ledgerId2, 0).nioBuffer());
        entryLogger.addEntry(ledgerId3, generateEntry(ledgerId3, 0).nioBuffer());
        entryLogger.addEntry(ledgerId1, generateEntry(ledgerId1, 1).nioBuffer());

        // Flush to ensure data is written
        entryLogger.flush();

        // Get entry log manager
        EntryLogManager entryLogManager = entryLogger.getEntryLogManager();
        assertTrue("Entry log manager should be EntryLogManagerForEntryLogPerLedger",
                entryLogManager instanceof EntryLogManagerForEntryLogPerLedger);

        // Shutdown the entry logger - this should call appendLedgersMap() for all active logs
        entryLogger.close();

        // Verify that log files have ledger metadata
        File ledgerDir = curDir;
        File[] logFiles = ledgerDir.listFiles((dir, name) -> name.endsWith(".log"));
        assertNotNull("Log files should exist", logFiles);
        assertTrue("Should have created log files", logFiles.length > 0);

        // Create new entry logger to read the metadata
        DefaultEntryLogger newEntryLogger = new DefaultEntryLogger(conf, dirsMgr);

        boolean foundLedgerMetadata = false;
        try {
            for (File logFile : logFiles) {
                String fileName = logFile.getName();
                String logIdStr = fileName.substring(0, fileName.indexOf(".log"));
                long logId;

                try {
                    // Try decimal first (most common)
                    logId = Long.parseLong(logIdStr);
                } catch (NumberFormatException e) {
                    // Try hex if decimal fails
                    logId = Long.parseLong(logIdStr, 16);
                }

                try {
                    EntryLogMetadata metadata = newEntryLogger.getEntryLogMetadata(logId);
                    if (metadata != null && metadata.getLedgersMap().size() > 0) {
                        foundLedgerMetadata = true;
                        LOG.info("SUCCESS: Entry log {} contains ledger metadata after shutdown", logId);
                    }
                } catch (IOException e) {
                    // Some logs might be empty, that's okay
                    LOG.debug("Could not read metadata from log {}: {}", logId, e.getMessage());
                }
            }
        } finally {
            newEntryLogger.close();
        }

        assertTrue("At least one entry log should contain ledger metadata after shutdown",
                  foundLedgerMetadata);
    }

    /**
     * Test that empty logs don't get metadata appended (conditional behavior).
     */
    @Test
    public void testEmptyLogShutdown() throws Exception {
        conf.setEntryLogPerLedgerEnabled(false);

        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, dirsMgr);

        // Don't add any entries - log should remain empty
        EntryLogManager entryLogManager = entryLogger.getEntryLogManager();
        long currentLogId = ((EntryLogManagerForSingleEntryLog) entryLogManager).getCurrentLogId();

        // Shutdown - should not append metadata for empty log
        entryLogger.close();

        // Verify behavior with empty log
        File logFile = new File(curDir, currentLogId + ".log");

        if (logFile.exists() && logFile.length() > 0) {
            DefaultEntryLogger newEntryLogger = new DefaultEntryLogger(conf, dirsMgr);
            try {
                EntryLogMetadata metadata = newEntryLogger.getEntryLogMetadata(currentLogId);
                // Empty log should either have no metadata or empty metadata
                assertTrue("Empty log should have empty ledger map",
                          metadata == null || metadata.getLedgersMap().size() == 0);
                LOG.info("SUCCESS: Empty entry log {} correctly handled during shutdown", currentLogId);
            } catch (IOException e) {
                // Expected for empty logs without metadata
                LOG.info("SUCCESS: Empty entry log {} has no metadata as expected", currentLogId);
            } finally {
                newEntryLogger.close();
            }
        } else {
            LOG.info("SUCCESS: No log file created for empty entry logger");
        }
    }
}