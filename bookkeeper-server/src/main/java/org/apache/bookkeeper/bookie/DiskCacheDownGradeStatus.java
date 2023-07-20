package org.apache.bookkeeper.bookie;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class DiskCacheDownGradeStatus {
    // whether the GC thread is in force GC.
    private boolean diskCacheDowngrading;
    private String ledgerDir;
    private String coldLedgerDir;

}