package top.thinkin.lightd.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import top.thinkin.lightd.base.DBCommandChunk;

public class DBClosure implements Closure {

    private DBCommandChunk chunk;

    private String msg;
    private int code;

    public DBClosure() {

    }

    @Override
    public void run(Status status) {
        if (!status.isOk()) {
            msg = status.getErrorMsg();
            code = status.getCode();
            synchronized (this) {
                this.notifyAll();
                System.out.println("notifyAll");
            }
        } else {
            code = 0;
        }
    }

    public DBCommandChunk getChunk() {
        return chunk;
    }

    public void setChunk(DBCommandChunk chunk) {
        this.chunk = chunk;
    }

    public String getMsg() {
        return msg;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
