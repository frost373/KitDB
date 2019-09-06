package top.thinkin.lightd.db;

import org.rocksdb.*;
import top.thinkin.lightd.base.DBCommand;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.base.TableConfig;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class DBAbs {
    protected RocksDB rocksDB;

    public RocksDB rocksDB() {
        return this.rocksDB;
    }

    protected WriteOptions writeOptions;

    protected ColumnFamilyHandle metaHandle;
    protected ColumnFamilyHandle defHandle;


    protected ThreadLocal<ReadOptions> readOptionsThreadLocal = new ThreadLocal<>();


    public ReadOptions getSnapshot() {
        return readOptionsThreadLocal.get();
    }


    protected ThreadLocal<List<DBCommand>> threadLogs = new ThreadLocal<>();

    public void start() {
        List<DBCommand> logs = threadLogs.get();
        if (logs == null) {
            logs = new ArrayList<>();
            threadLogs.set(logs);
        }
        logs.clear();
    }

    public void commit(List<DBCommand> logs) throws Exception {
        try (final WriteBatch batch = new WriteBatch()) {
            setLogs(logs, batch);
            this.rocksDB().write(this.writeOptions(), batch);
        } catch (Exception e) {
            throw e;
        }
    }

    protected WriteOptions writeOptions() {
        return this.writeOptions;
    }


    private void setLogs(List<DBCommand> logs, WriteBatch batch) throws RocksDBException {
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
    }


    public interface FunctionCommit {
        void call(List<DBCommand> logs) throws Exception;
    }

    public FunctionCommit functionCommit = logs -> commit(logs);

    private static List<ColumnFamilyDescriptor> getColumnFamilyDescriptor() {
        final ColumnFamilyOptions cfOptions = TableConfig.createColumnFamilyOptions();
        final ColumnFamilyOptions defCfOptions = TableConfig.createDefColumnFamilyOptions();
        final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        cfDescriptors.add(new ColumnFamilyDescriptor("R_META".getBytes(), cfOptions));
        cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defCfOptions));

        return cfDescriptors;
    }


    public void commit() throws Exception {
        List<DBCommand> logs = threadLogs.get();
        try {
            functionCommit.call(logs);
        } catch (Exception e) {
            throw e;
        } finally {
            logs.clear();
        }
    }

    private ColumnFamilyHandle findColumnFamilyHandle(final SstColumnFamily sstColumnFamily) {
        switch (sstColumnFamily) {
            case DEFAULT:
                return this.defHandle;
            case META:
                return this.metaHandle;
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
        if (this.getSnapshot() != null) {
            return this.rocksDB().get(findColumnFamilyHandle(columnFamily), this.getSnapshot(), key);
        }
        return this.rocksDB().get(findColumnFamilyHandle(columnFamily), key);
    }


    protected RocksIterator newIterator(SstColumnFamily columnFamily) {
        if (this.getSnapshot() != null) {
            return this.rocksDB().newIterator(findColumnFamilyHandle(columnFamily), this.getSnapshot());
        }
        return this.rocksDB().newIterator(findColumnFamilyHandle(columnFamily));
    }


    protected Map<byte[], byte[]> multiGet(List<byte[]> keys, SstColumnFamily columnFamily) throws RocksDBException {

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            columnFamilyHandles.add(findColumnFamilyHandle(columnFamily));
        }
        if (this.getSnapshot() != null) {

            ReadOptions readOptions = new ReadOptions();
            readOptions.setSnapshot(this.getSnapshot().snapshot());
            return this.rocksDB().multiGet(readOptions, Arrays.asList(findColumnFamilyHandle(columnFamily)), keys);
        }
        return this.rocksDB().multiGet(columnFamilyHandles, keys);
    }


   /* protected void deleteHead2(byte[] head, SstColumnFamily columnFamily) {

        ReadOptions readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true);

        try (final RocksIterator iterator = this.rocksDB().newIterator(findColumnFamilyHandle(columnFamily),readOptions)) {
            iterator.seek(head);
            byte[] start;
            byte[] end;
            if (iterator.isValid()) {
                start = iterator.key();
                if (BytesUtil.checkHead(head, start)) {
                    iterator.seek(head);
                    iterator.seekToLast();
                    end = iterator.key();
                    if (BytesUtil.checkHead(head, end)) {
                        deleteRangeDB(start, end, columnFamily);
                        deleteDB(end, columnFamily);
                    } else {
                        iterator.prev();
                        end = iterator.key();
                        if (BytesUtil.checkHead(head, end)) {
                            deleteRangeDB(start, end, columnFamily);
                            deleteDB(end, columnFamily);
                        }{
                            deleteDB(start, columnFamily);
                        }
                    }
                }
            }
        }
    }*/


    protected void deleteHead(byte[] head, SstColumnFamily columnFamily) {

        ReadOptions readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true);
        try (final RocksIterator iterator = this.rocksDB().newIterator(findColumnFamilyHandle(columnFamily), readOptions)) {
            iterator.seek(head);
            byte[] start;
            byte[] end = null;
            start = iterator.key();
            if (!BytesUtil.checkHead(head, start)) return;
            while (iterator.isValid()) {
                if (!BytesUtil.checkHead(head, start)) break;
                end = iterator.key();
                iterator.next();
            }
            if (end != null) {
                deleteRangeDB(start, end, columnFamily);
            }
            deleteDB(end, columnFamily);
        }
    }

}
