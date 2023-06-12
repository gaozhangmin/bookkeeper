package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BookieDemo {

    public static void main(String[] args) throws Exception {
        BookKeeper bkc = new BookKeeper("127.0.0.1:2181");

        LedgerHandle lh = bkc.createLedger(2, 1, 1, BookKeeper.DigestType.MAC, "".getBytes(StandardCharsets.UTF_8));
        long legerId  = lh.getId();
        ByteBuffer entry = ByteBuffer.allocate(4);

        int numberOfEntries = 2;
        for (int i = 0; i < numberOfEntries; i++) {
            entry.putInt(i);
            entry.position(0);
            lh.addEntry(entry.array());
        }

        LedgerHandle lh1 = bkc.openLedger(legerId, BookKeeper.DigestType.MAC, "".getBytes(StandardCharsets.UTF_8));


        bkc.close();


    }

    private static int realRefCnt(int rawCnt) {
        return rawCnt != 2 && rawCnt != 4 && (rawCnt & 1) != 0 ? 0 : rawCnt >>> 1;
    }

}
