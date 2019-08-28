package top.thinkin.lightd.db;

import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import top.thinkin.lightd.base.DBCommand;
import top.thinkin.lightd.kit.BytesUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class RBase {
    //protected byte[] key_b;
    protected DB db;

    protected final boolean isLog;

    protected static Charset charset = Charset.forName("UTF-8");
    //private List<DBCommand> logs = new ArrayList<>();
    private ThreadLocal<List<DBCommand>> threadLogs = new ThreadLocal<>();


    public RBase(boolean isLog) {
        this.isLog = isLog;
    }

    public RBase() {
        this.isLog = false;
    }

    public void start() {
        List<DBCommand> logs = threadLogs.get();
        if (logs == null) {
            logs = new ArrayList<>();
            threadLogs.set(logs);
        }
        logs.clear();
    }

    public void commit() throws Exception {
        List<DBCommand> logs = threadLogs.get();
        try (final WriteBatch batch = new WriteBatch()) {
            for (DBCommand log : logs) {
                switch (log.getType()) {
                    case DELETE:
                        batch.delete(log.getKey());
                        break;
                    case UPDATE:
                        batch.put(log.getKey(), log.getValue());
                        break;
                    case DELETE_RANGE:
                        batch.deleteRange(log.getStart(), log.getEnd());
                        break;
                }
            }
            db.rocksDB().write(db.writeOptions(), batch);
            //db.getBinLog().addLog( logs.stream().map(DBCommand::toBytes).collect(Collectors.toList()));
        } catch (Exception e) {
            throw e;
        } finally {
            logs.clear();
        }
    }

    public void release() {
        List<DBCommand> logs = threadLogs.get();
        if (logs != null) {
            logs.clear();
        }
    }

    public void putDB(byte[] key, byte[] value) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.update(key, value));
    }

    public void deleteDB(byte[] key) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.delete(key));
    }

    protected void deleteRangeDB(byte[] start, byte[] end) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.deleteRange(start, end));
    }

    protected static void deleteHead(byte[] head, RBase rBase) {
        try (final RocksIterator iterator = rBase.db.rocksDB().newIterator()) {
            iterator.seek(head);
            byte[] start;
            byte[] end;
            if (iterator.isValid()) {
                start = iterator.key();
                iterator.prev();
                if (BytesUtil.checkHead(head, start)) {
                    iterator.seekToLast();
                    end = iterator.key();
                    if (BytesUtil.checkHead(head, end)) {
                        rBase.deleteRangeDB(start, end);
                        rBase.deleteDB(end);
                    } else {
                        rBase.deleteDB(start);
                    }
                }
            }
        }
    }


}
