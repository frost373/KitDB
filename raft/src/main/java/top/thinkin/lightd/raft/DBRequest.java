package top.thinkin.lightd.raft;

import top.thinkin.lightd.base.DBCommand;

import java.io.Serializable;
import java.util.List;

public class DBRequest implements Serializable {
    private static final long serialVersionUID = -5623664785560971849L;
    private String key;
    private List<DBCommand> logs;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<DBCommand> getLogs() {
        return logs;
    }

    public void setLogs(List<DBCommand> logs) {
        this.logs = logs;
    }
}
