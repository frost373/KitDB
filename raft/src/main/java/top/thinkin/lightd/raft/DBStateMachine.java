package top.thinkin.lightd.raft;

import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import top.thinkin.lightd.base.DBCommand;
import top.thinkin.lightd.db.DB;

import java.nio.ByteBuffer;
import java.util.List;

public class DBStateMachine extends StateMachineAdapter {

    private DB rBase;

    private DBRequestProcessor dbRequestProcessor;

    public void setDbRequestProcessor(DBRequestProcessor dbRequestProcessor) {
        this.dbRequestProcessor = dbRequestProcessor;
    }

    public DBRequestProcessor getDbRequestProcessor() {
        return dbRequestProcessor;
    }

    public void setrBase(DB rBase) {
        this.rBase = rBase;
        rBase.functionCommit = logs -> dbRequestProcessor.call(logs);
    }

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            final ByteBuffer data = iter.getData();
            DBClosure closure = null;
            try {
                List<DBCommand> logs = null;
                if (iter.done() != null) {
                    closure = (DBClosure) iter.done();
                    logs = closure.getLogs();

                } else {
                    try {
                        final DBRequest request = SerializerManager.getSerializer(SerializerManager.Hessian2)
                                .deserialize(data.array(), DBRequest.class.getName());
                        logs = request.getLogs();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                try {
                    rBase.commit(logs);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (closure != null) {
                    closure.run(Status.OK());
                    closure.notify();
                }
            } finally {
                if (closure != null) {
                    closure.notify();
                }
            }
        }
    }
}
