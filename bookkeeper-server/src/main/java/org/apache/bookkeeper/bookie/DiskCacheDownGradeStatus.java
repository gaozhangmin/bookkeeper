package org.apache.bookkeeper.bookie;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DiskCacheDownGradeStatus {
    // whether the GC thread is in force GC.
    private boolean diskCacheDowngrading;
    private String ledgerDir;
    private String coldLedgerDir;

}
