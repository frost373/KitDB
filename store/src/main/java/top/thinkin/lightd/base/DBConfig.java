package top.thinkin.lightd.base;

import lombok.Data;

@Data
public class DBConfig {
    private String dbDir;
    private boolean autoClear = true;
    private boolean needBinLog = false;
    private String binLogDir;
    private String snapshotDir;
}
