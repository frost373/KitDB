package top.thinkin.lightd.raft;

import lombok.Data;

@Data
public class NodeConfig {
    private String raftDir;
    private String node;
}
