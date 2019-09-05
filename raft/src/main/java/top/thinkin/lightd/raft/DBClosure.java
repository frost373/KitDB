package top.thinkin.lightd.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import top.thinkin.lightd.base.DBCommand;

import java.util.List;

public class DBClosure implements Closure {

    private List<DBCommand> logs;


    public DBClosure() {

    }

    @Override
    public void run(Status status) {

    }

    public List<DBCommand> getLogs() {
        return logs;
    }

    public void setLogs(List<DBCommand> logs) {
        this.logs = logs;
    }
}
