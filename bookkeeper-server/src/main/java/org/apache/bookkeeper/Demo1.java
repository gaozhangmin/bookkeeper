package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Demo1 {

    public static void main(String[] args) throws Exception {

        ClientConfiguration clientConf = new ClientConfiguration().setMetadataServiceUri(
                "zk+null://127.0.0.1:2181/ledgers");
        clientConf.setUseV2WireProtocol(true);
        BookKeeper bkc = new BookKeeper(clientConf);

        LedgerHandle lh = bkc.createLedger(1, 1, 1, BookKeeper.DigestType.MAC, "".getBytes(StandardCharsets.UTF_8));

        ByteBuffer entry = ByteBuffer.allocate(4);

        int numberOfEntries = 10;
        for (int j = 0; j < numberOfEntries; j++) {
            entry.put("aaaa".getBytes());
            entry.position(0);
            lh.addEntry(entry.array());
            System.out.println(lh.getLastAddConfirmed() + ", " + lh.getLastAddPushed() + ", " + lh.readLastAddConfirmed());
        }


        System.in.read();
    }

}
