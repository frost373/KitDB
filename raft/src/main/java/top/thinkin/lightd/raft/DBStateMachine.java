package top.thinkin.lightd.raft;

import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.thinkin.lightd.base.DBCommandChunk;
import top.thinkin.lightd.base.DBCommandChunkType;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.KitDBException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class DBStateMachine extends StateMachineAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(DBStateMachine.class);

    volatile private DB db;

    public DB getDb() {
        return db;
    }

    private static String spname = "sp";

    private DBRequestProcessor dbRequestProcessor;

    private String dbName;


    private ConcurrentHashMap<String, List<DBCommandChunk>> logbatchs = new ConcurrentHashMap<>();

    // private List<DBCommandChunk> logbatch = new ArrayList<>();

    private final AtomicLong leaderTerm = new AtomicLong(-1L);


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


    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onError(RaftException e) {
        LOG.error("onSnapshotLoad error", e);
    }


    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        try {
            String fileName = this.db.backupDB(writer.getPath(), spname);
            if (writer.addFile(spname + DB.BACK_FILE_SUFFIX)) {
                done.run(Status.OK());
            } else {
                done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
            }
        } catch (Exception e) {
            done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", writer.getPath()));
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
    }


    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        this.leaderTerm.set(-1L);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        LOG.info("===========onSnapshotLoad start==============");
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        String path = reader.getPath();


        if (db != null) {
            try {
                db.stop();
            } catch (Exception e) {
                LOG.error("onSnapshotLoad error", e);
                return false;
            }
        }

        Util.delZSPic(db.getDir());

        try {
            DB.releaseBackup(path + File.separator + spname + DB.BACK_FILE_SUFFIX, db.getDir());
            db.open(false, false);
            return true;
        } catch (Exception e) {
            LOG.error("onSnapshotLoad error", e);
            return false;
        }

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
                        LOG.error("Get Serialize ERROR", e);
                    }
                }

                try {
                    DBCommandChunkType dbCommandChunkType = chunk.getType();
                    if (chunk.getCommands() != null) {
                        LOG.debug("onApply {} {}", dbCommandChunkType.name(), chunk.getCommands().size());
                    } else {
                        LOG.debug("onApply {} {}", dbCommandChunkType.name());
                    }
                    if (!isLeader) {
                        switch (dbCommandChunkType) {
                            case NOM_COMMIT:
                                //普通提交-直接写入
                                db.simpleCommit(chunk.getCommands());
                                break;
                            case TX_LOGS:
                                //事物写入-写入缓冲区

                                List<DBCommandChunk> logbatch_logs = logbatchs.get(chunk.getEntity().getUuid());
                                if (logbatch_logs == null) {
                                    logbatch_logs = new ArrayList<>();
                                    logbatchs.put(chunk.getEntity().getUuid(), logbatch_logs);
                                }
                                logbatch_logs.add(chunk);
                                break;
                            case TX_COMMIT:
                                //事物提交-直接写入
                                List<DBCommandChunk> logbatch_commit = logbatchs.get(chunk.getEntity().getUuid());
                                if (logbatch_commit != null) {
                                    for (DBCommandChunk dbCommandChunk : logbatch_commit) {
                                        db.simpleCommit(dbCommandChunk.getCommands());
                                    }
                                }
                                logbatchs.remove(chunk.getEntity().getUuid());
                                break;
                            case TX_ROLLBACK:
                                //db.rollbackTX(chunk.getEntity());
                                logbatchs.remove(chunk.getEntity().getUuid());
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
                                db.commit(chunk.getCommands(), chunk.getEntity());
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

                } catch (Exception e) {
                    LOG.error("STORE ERROR", e);
                    closure.run(new Status(-1, e.getMessage()));
                    return;
                }
                if (closure != null) {
                    closure.run(Status.OK());

                }

            } finally {
                if (closure != null) {
                    synchronized (closure) {
                        closure.notifyAll();
                    }
                }
            }
            iter.next();
        }
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
