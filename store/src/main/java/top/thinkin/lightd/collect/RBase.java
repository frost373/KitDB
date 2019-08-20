package top.thinkin.lightd.collect;

import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class RBase {
    protected byte[] key_b;
    protected DB db;
    protected static Charset charset = Charset.forName("UTF-8");
    //private List<DBLog> logs = new ArrayList<>();
    private ThreadLocal<List<DBLog>> threadLogs = new ThreadLocal<>();

    public void start() {
        List<DBLog> logs = threadLogs.get();
        if (logs == null) {
            logs = new ArrayList<>();
            threadLogs.set(logs);
        }
        logs.clear();
    }

    public void commit() throws Exception {
        List<DBLog> logs = threadLogs.get();
        try (final WriteBatch batch = new WriteBatch()) {
            for (DBLog log : logs) {
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
        } catch (Exception e) {
            throw e;
        } finally {
            logs.clear();
        }
    }

    public void release() {
        List<DBLog> logs = threadLogs.get();
        if (logs != null) {
            logs.clear();
        }
    }

    protected void putDB(byte[] key, byte[] value) {
        List<DBLog> logs = threadLogs.get();
        logs.add(DBLog.update(key, value));
    }

    protected void deleteDB(byte[] key) {
        List<DBLog> logs = threadLogs.get();
        logs.add(DBLog.delete(key));
    }

    protected void deleteRangeDB(byte[] start, byte[] end) {
        List<DBLog> logs = threadLogs.get();
        logs.add(DBLog.deleteRange(start, end));
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
