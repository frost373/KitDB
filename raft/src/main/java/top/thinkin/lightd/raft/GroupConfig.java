package top.thinkin.lightd.raft;

import lombok.Data;

@Data
public class GroupConfig {
    private String group;
    private String initNodes;
    private int electionTimeoutMs = 1000;
    private int snapshotIntervalSecs = 3600;
}
