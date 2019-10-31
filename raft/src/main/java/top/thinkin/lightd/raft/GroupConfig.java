package top.thinkin.lightd.raft;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class GroupConfig {
    private String group;
    private String initNodes;
    private int electionTimeoutMs = 1000;
    private int snapshotIntervalSecs = 3600;
}
