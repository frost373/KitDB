package top.thinkin.lightd.raft;

import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftException;
import top.thinkin.lightd.base.DBCommandChunk;
import top.thinkin.lightd.base.DBCommandChunkType;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.KitDBException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DBStateMachine extends StateMachineAdapter {

    private DB db;

    private DBRequestProcessor dbRequestProcessor;


    private List<DBCommandChunk> logbatch = new ArrayList<>();

    public void setDbRequestProcessor(DBRequestProcessor dbRequestProcessor) {
        this.dbRequestProcessor = dbRequestProcessor;
    }

    public DBRequestProcessor getDbRequestProcessor() {
        return dbRequestProcessor;
    }

    public void setDB(DB db) {
        this.db = db;
        db.functionCommit = logs -> dbRequestProcessor.call(logs);
    }

    @Override
    public void onError(RaftException e) {
        e.getStatus();
    }

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            final ByteBuffer data = iter.getData();
            DBClosure closure = null;
            boolean isLeader = false;
            try {
                DBCommandChunk chunk = null;
                if (iter.done() != null) {
                    closure = (DBClosure) iter.done();
                    chunk = closure.getChunk();
                    isLeader = true;
                } else {
                    try {
                        final DBCommandChunk request = SerializerManager.getSerializer(SerializerManager.Hessian2)
                                .deserialize(data.array(), DBCommandChunk.class.getName());
                        chunk = request;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                try {
                    DBCommandChunkType dbCommandChunkType = chunk.getType();
                    System.out.println("wri");
                    if (!isLeader) {
                        switch (dbCommandChunkType) {
                            case NOM_COMMIT:
                                //普通提交-直接写入
                                db.simpleCommit(chunk.getCommands());
                                break;
                            case TX_LOGS:
                                //事物写入-写入缓冲区
                                logbatch.add(chunk);
                                break;
                            case TX_COMMIT:
                                //事物提交-直接写入
                                for (DBCommandChunk dbCommandChunk : logbatch) {
                                    db.simpleCommit(dbCommandChunk.getCommands());
                                }
                                break;
                            case TX_ROLLBACK:
                                //db.rollbackTX(chunk.getEntity());
                                break;
                            case SIMPLE_COMMIT:
                                //普通提交-直接写入
                                db.simpleCommit(chunk.getCommands());
                                break;
                        }
                    } else {
                        switch (dbCommandChunkType) {
                            case NOM_COMMIT:
                                db.simpleCommit(chunk.getCommands());
                                break;
                            case TX_LOGS:
                                break;
                            case TX_COMMIT:
                                db.commitTX(chunk.getEntity());
                                break;
                            case TX_ROLLBACK:
                                db.rollbackTX(chunk.getEntity());
                                break;
                            case SIMPLE_COMMIT:
                                db.simpleCommit(chunk.getCommands());
                                break;
                            default:
                                throw new KitDBException(ErrorType.NULL, "DBCommandChunkType non-existent!");
                        }


                    }

                    // 非事物模式
                    // leader:
                    // 事物模式
                    //leader:事物 提交 回滚

                    //只记录不提交
                    //一次性提交

                    //db.commit(logs);

                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (closure != null) {
                    closure.run(Status.OK());
                    /*synchronized (closure) {
                        closure.notify();
                    }*/
                }

            } finally {
                if (closure != null) {
                    synchronized (closure) {
                        closure.notifyAll();
                        System.out.println("notifyAll");
                    }
                }
            }
            iter.next();
        }
    }
}
