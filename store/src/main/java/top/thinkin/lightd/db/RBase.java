package top.thinkin.lightd.db;

import org.rocksdb.*;
import top.thinkin.lightd.base.DBCommand;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.kit.BytesUtil;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class RBase {
    //protected byte[] key_b;
    protected DB db;

    protected final boolean isLog;

    protected static Charset charset = Charset.forName("UTF-8");
    private ThreadLocal<List<DBCommand>> threadLogs = new ThreadLocal<>();

    private ReadOptions readOptions = new ReadOptions();

    private ThreadLocal<ReadOptions> readOptionsThreadLocal = new ThreadLocal<>();

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
                        batch.delete(findColumnFamilyHandle(log.getFamily()), log.getKey());
                        break;
                    case UPDATE:
                        batch.put(findColumnFamilyHandle(log.getFamily()), log.getKey(), log.getValue());
                        break;
                    case DELETE_RANGE:
                        batch.deleteRange(findColumnFamilyHandle(log.getFamily()), log.getStart(), log.getEnd());
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

    private ColumnFamilyHandle findColumnFamilyHandle(final SstColumnFamily sstColumnFamily) {
        switch (sstColumnFamily) {
            case DEFAULT:
                return this.db.defHandle;
            case META:
                return this.db.metaHandle;
            default:
                throw new IllegalArgumentException("illegal sstColumnFamily: " + sstColumnFamily.name());
        }
    }


    public void release() {
        List<DBCommand> logs = threadLogs.get();
        if (logs != null) {
            logs.clear();
        }
    }


    public void putDB(byte[] key, byte[] value, SstColumnFamily columnFamily) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.update(key, value, columnFamily));
    }

    public void deleteDB(byte[] key, SstColumnFamily columnFamily) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.delete(key, columnFamily));
    }


    protected void deleteRangeDB(byte[] start, byte[] end, SstColumnFamily columnFamily) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.deleteRange(start, end, columnFamily));
    }


    protected byte[] getDB(byte[] key, SstColumnFamily columnFamily) throws RocksDBException {
        if (db.getSnapshot() != null) {
            return db.rocksDB().get(findColumnFamilyHandle(columnFamily), db.getSnapshot(), key);
        }
        return db.rocksDB().get(findColumnFamilyHandle(columnFamily), key);
    }


    protected RocksIterator newIterator(SstColumnFamily columnFamily) {
        if (db.getSnapshot() != null) {
            return db.rocksDB().newIterator(findColumnFamilyHandle(columnFamily), db.getSnapshot());
        }
        return db.rocksDB().newIterator(findColumnFamilyHandle(columnFamily));
    }


    protected Map<byte[], byte[]> multiGet(List<byte[]> keys, SstColumnFamily columnFamily) throws RocksDBException {
        if (db.getSnapshot() != null) {

            ReadOptions readOptions = new ReadOptions();
            readOptions.setSnapshot(db.getSnapshot().snapshot());
            return db.rocksDB().multiGet(readOptions, Arrays.asList(findColumnFamilyHandle(columnFamily)), keys);
        }
        return db.rocksDB().multiGet(Arrays.asList(findColumnFamilyHandle(columnFamily)), keys);
    }

    protected Map<byte[], byte[]> multiGet(List<byte[]> keys) throws RocksDBException {
        if (db.getSnapshot() != null) {
            return db.rocksDB().multiGet(db.getSnapshot(), keys);
        }
        return db.rocksDB().multiGet(keys);
    }


    protected static void deleteHead(byte[] head, RBase rBase, SstColumnFamily columnFamily) {
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
                        rBase.deleteRangeDB(start, end, columnFamily);
                        rBase.deleteDB(end, columnFamily);
                    } else {
                        rBase.deleteDB(start, columnFamily);
                    }
                }
            }
        }
    }


}
