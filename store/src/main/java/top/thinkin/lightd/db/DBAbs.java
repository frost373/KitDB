package top.thinkin.lightd.db;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import top.thinkin.lightd.base.*;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class DBAbs {
    protected RocksDB rocksDB;
    protected boolean openTransaction = false;
    protected KeySegmentLockManager keySegmentLockManager;

    protected RocksDB getRocksDB() {
        return rocksDB;
    }

    public RocksDB rocksDB() {
        return this.rocksDB;
    }

    protected WriteOptions writeOptions;
    protected ReadOptions readOptions = new ReadOptions();

    protected ColumnFamilyHandle metaHandle;
    protected ColumnFamilyHandle defHandle;

    protected ThreadLocal<List<DBCommand>> threadLogs = new ThreadLocal<>();

    protected ThreadLocal<TransactionEntity> TRANSACTION_ENTITY = new ThreadLocal<>();

    protected ThreadLocal<Boolean> IS_STATR_TX = ThreadLocal.withInitial(() -> false);
    protected final ReentrantLock TX_LOCK = new ReentrantLock(true);

    protected DBAbs() {

    }

    public void commitTX() throws Exception {
        DAssert.isTrue(this.IS_STATR_TX.get(), ErrorType.TX_NOT_START, "Transaction have not started");
        TransactionEntity entity = TRANSACTION_ENTITY.get();

        if (entity.getCount() > 0) {
            //事务不需要提交，计数器减一
            entity.subCount();
        } else {
            try {
                entity.getTransaction().commit();
            } finally {
                IS_STATR_TX.set(false);
                TX_LOCK.unlock();
                entity.reset();
            }
        }
    }


    public void rollbackTX() throws RocksDBException {
        if (!this.IS_STATR_TX.get()) {
            return;
        }
        TransactionEntity entity = TRANSACTION_ENTITY.get();
        if (entity.getCount() > 0) {
            //事务不需要提交，计数器减一
            entity.subCount();
        } else {
            try {
                entity.getTransaction().rollback();
            } finally {
                IS_STATR_TX.set(false);
                TX_LOCK.unlock();
                entity.reset();
            }
        }

    }

    public void start() {
        List<DBCommand> logs = threadLogs.get();
        if (logs == null) {
            logs = new ArrayList<>();
            threadLogs.set(logs);
        }
        logs.clear();
    }


    public void startTran(int waitTime) throws InterruptedException {
        DAssert.isTrue(this.openTransaction, ErrorType.NOT_TX_DB, "this db is not a Transaction DB");
        if (!this.IS_STATR_TX.get()) {
            if (TX_LOCK.tryLock(waitTime, TimeUnit.MILLISECONDS)) {
                TransactionEntity transactionEntity = new TransactionEntity();
                TransactionDB rocksDB = (TransactionDB) this.rocksDB();
                Transaction transaction = rocksDB.beginTransaction(this.writeOptions);
                transactionEntity.setTransaction(transaction);
                TRANSACTION_ENTITY.set(transactionEntity);
                IS_STATR_TX.set(true);
            } else {
                DAssert.isTrue(false, ErrorType.TX_GET_TIMEOUT, "Waiting to get Transaction timed out");
            }
        } else {
            TRANSACTION_ENTITY.get().addCount();
        }
    }

    public void checkKey() {
        DAssert.isTrue(this.openTransaction, ErrorType.NOT_TX_DB, "this db is not a Transaction DB");
        DAssert.isTrue(!TX_LOCK.isLocked(), ErrorType.TX_ERROR, "a Transaction is being executed");
    }

    protected void commit(List<DBCommand> logs) throws Exception {

        if (this.IS_STATR_TX.get()) {
            Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
            try (final WriteBatch batch = new WriteBatch()) {
                setLogs(logs, batch);
                transaction.rebuildFromWriteBatch(batch);
            } catch (Exception e) {
                throw e;
            }
        } else {
            try (final WriteBatch batch = new WriteBatch()) {
                setLogs(logs, batch);
                this.rocksDB().write(this.writeOptions(), batch);
            } catch (Exception e) {
                throw e;
            }
        }


    }

    protected void commit() throws Exception {
        List<DBCommand> logs = threadLogs.get();
        try {
            functionCommit.call(logs);
        } catch (Exception e) {
            throw e;
        } finally {
            logs.clear();
        }
    }


    protected WriteOptions writeOptions() {
        return this.writeOptions;
    }


    protected void release() {
        List<DBCommand> logs = threadLogs.get();
        if (logs != null) {
            logs.clear();
        }
    }

    protected void putDB(byte[] key, byte[] value, SstColumnFamily columnFamily) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.update(key, value, columnFamily));
    }

    protected void deleteDB(byte[] key, SstColumnFamily columnFamily) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.delete(key, columnFamily));
    }


    protected void deleteRangeDB(byte[] start, byte[] end, SstColumnFamily columnFamily) {
        List<DBCommand> logs = threadLogs.get();
        logs.add(DBCommand.deleteRange(start, end, columnFamily));
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

    protected static List<ColumnFamilyDescriptor> getColumnFamilyDescriptor() {
        final ColumnFamilyOptions cfOptions = TableConfig.createColumnFamilyOptions();
        final ColumnFamilyOptions defCfOptions = TableConfig.createDefColumnFamilyOptions();
        final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        cfDescriptors.add(new ColumnFamilyDescriptor("R_META".getBytes(), cfOptions));
        cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defCfOptions));

        return cfDescriptors;
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


    protected byte[] getDB(byte[] key, SstColumnFamily columnFamily) throws RocksDBException {
        if (this.IS_STATR_TX.get()) {
            Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
            return transaction.get(findColumnFamilyHandle(columnFamily), readOptions, key);
        }
        return this.rocksDB().get(findColumnFamilyHandle(columnFamily), key);
    }


    protected RocksIterator newIterator(SstColumnFamily columnFamily) {

        if (this.IS_STATR_TX.get()) {
            Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
            return transaction.getIterator(readOptions, findColumnFamilyHandle(columnFamily));
        }

        return this.rocksDB().newIterator(findColumnFamilyHandle(columnFamily));
    }


    protected Map<byte[], byte[]> multiGet(List<byte[]> keys, SstColumnFamily columnFamily) throws RocksDBException {

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
        for (byte[] ignored : keys) {
            columnFamilyHandles.add(findColumnFamilyHandle(columnFamily));
        }
        if (this.IS_STATR_TX.get()) {
            Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
            byte[][] bytes = keys.toArray(new byte[keys.size()][]);
            transaction.multiGet(readOptions, columnFamilyHandles, bytes);
            return this.rocksDB().multiGet(readOptions, columnFamilyHandles, keys);
        }
        return this.rocksDB().multiGet(columnFamilyHandles, keys);
    }


    protected void deleteHead(byte[] head, SstColumnFamily columnFamily) {
        final RocksIterator iterator;
        ReadOptions readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true);
        iterator = this.rocksDB().newIterator(findColumnFamilyHandle(columnFamily), readOptions);

        try {
            iterator.seek(head);
            byte[] start;
            byte[] end = null;
            start = iterator.key();
            if (!BytesUtil.checkHead(head, start)) return;
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (!BytesUtil.checkHead(head, key)) break;
                end = key;
                iterator.next();
            }
            if (end != null) {
                deleteRangeDB(start, end, columnFamily);
            }
            deleteDB(end, columnFamily);
        } finally {
            iterator.close();
        }
    }

}
