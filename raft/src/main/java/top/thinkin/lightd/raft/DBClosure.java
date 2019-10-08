package top.thinkin.lightd.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import top.thinkin.lightd.base.DBCommandChunk;

public class DBClosure implements Closure {

    private DBCommandChunk chunk;



    public DBClosure() {

    }

    @Override
    public void run(Status status) {

    }

    public DBCommandChunk getChunk() {
        return chunk;
    }

    public void setChunk(DBCommandChunk chunk) {
        this.chunk = chunk;
    }
}
