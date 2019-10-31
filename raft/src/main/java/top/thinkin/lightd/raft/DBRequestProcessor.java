package top.thinkin.lightd.raft;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.thinkin.lightd.base.DBCommandChunk;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.KitDBException;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class DBRequestProcessor implements DB.FunctionCommit {
    ConcurrentHashMap<Object, String> map = new ConcurrentHashMap<>();


    private static final Logger LOG = LoggerFactory.getLogger(DBRequestProcessor.class);

    private final KitRaft kitRaft;

    public DBRequestProcessor(KitRaft kitRaft) {
        this.kitRaft = kitRaft;
    }


    public void handle(DBCommandChunk dbCommandChunk) throws CodecException, InterruptedException, KitDBException {
        final DBClosure closure = new DBClosure();
        closure.setChunk(dbCommandChunk);
        final Task task = new Task();
        task.setDone(closure);
        task.setData(ByteBuffer
                .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(dbCommandChunk)));
        kitRaft.getNode().apply(task);
        synchronized (closure) {
            closure.wait();
        }
        if (closure.getCode() != 0) {
            throw new KitDBException(ErrorType.STROE_ERROR, closure.getMsg());
        }
        System.out.println("finish");
    }


    @Override
    public void call(DBCommandChunk dbCommandChunk) throws KitDBException {
        try {
            handle(dbCommandChunk);
        } catch (CodecException | InterruptedException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }
}
