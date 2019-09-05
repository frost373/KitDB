package top.thinkin.lightd.raft;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.thinkin.lightd.base.DBCommand;
import top.thinkin.lightd.db.DB;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DBRequestProcessor implements DB.FunctionCommit {
    ConcurrentHashMap<Object, String> map = new ConcurrentHashMap<>();


    private static final Logger LOG = LoggerFactory.getLogger(DBRequestProcessor.class);

    private final LightDServer lightDServer;

    public DBRequestProcessor(LightDServer lightDServer) {
        this.lightDServer = lightDServer;
    }

    @Override
    public void call(List<DBCommand> logs) throws Exception {

    }


    public void handle(List<String> logs) throws CodecException, InterruptedException {
        final DBClosure closure = new DBClosure();
        final Task task = new Task();
        task.setDone(closure);
        task.setData(ByteBuffer
                .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(logs)));
        lightDServer.getNode().apply(task);
        closure.wait();
    }


}
