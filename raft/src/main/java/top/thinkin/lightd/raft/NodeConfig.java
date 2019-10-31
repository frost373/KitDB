package top.thinkin.lightd.raft;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NodeConfig {
    private String raftDir;
    private String node;
}
