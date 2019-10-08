package top.thinkin.lightd.raft;

import top.thinkin.lightd.base.DBCommandChunk;

import java.io.Serializable;

public class DBRequest implements Serializable {
    private static final long serialVersionUID = -5623664785560971849L;
    private String key;
    private DBCommandChunk chunk;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public DBCommandChunk getChunk() {
        return chunk;
    }

    public void setChunk(DBCommandChunk chunk) {
        this.chunk = chunk;
    }
}
