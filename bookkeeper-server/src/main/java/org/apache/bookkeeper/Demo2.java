package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.nio.charset.StandardCharsets;
public class Demo2 {

    public static void main(String[] args) throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration().setMetadataServiceUri(
                "zk+null://127.0.0.1:2181/ledgers");
        clientConf.setUseV2WireProtocol(true);
        BookKeeper bkc = new BookKeeper(clientConf);

        LedgerHandle lh = bkc.openLedger(6, BookKeeper.DigestType.MAC, "".getBytes(StandardCharsets.UTF_8));
        lh.readEntries(0, 9);


        System.in.read();
    }

}
