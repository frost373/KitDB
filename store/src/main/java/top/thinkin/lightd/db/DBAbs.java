package top.thinkin.lightd.db;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import top.thinkin.lightd.base.*;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.KitDBException;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public abstract class DBAbs {
    protected RocksDB rocksDB;
    protected boolean openTransaction = false;
    protected volatile boolean open = false;

    protected KeySegmentLockManager keySegmentLockManager;

    public RocksDB rocksDB() {
        return this.rocksDB;
    }

    protected WriteOptions writeOptions;

    protected DBOptions options;



    protected ReadOptions readOptions = new ReadOptions();

    protected ColumnFamilyHandle metaHandle;
    protected ColumnFamilyHandle defHandle;

    protected ThreadLocal<List<DBCommand>> threadLogs = new ThreadLocal<>();

    protected ThreadLocal<TransactionEntity> TRANSACTION_ENTITY = new ThreadLocal<>();

    public final ThreadLocal<Boolean> IS_STATR_TX = ThreadLocal.withInitial(() -> false);

    protected final ReentrantLock TX_LOCK = new ReentrantLock(true);


    protected final ReadWriteLock CLOSE_LOCK = new ReentrantReadWriteLock(true);


    protected CloseLock closeCheck() throws KitDBException {
        Lock lock = CLOSE_LOCK.readLock();
        lock.lock();
        try {
            DAssert.isTrue(open, ErrorType.DB_CLOSE, "db is closed");
        } catch (KitDBException e) {
            lock.unlock();
            throw e;
        }
        return new CloseLock(lock);
    }

    protected CloseLock closeDo() throws KitDBException {
        Lock lock = CLOSE_LOCK.writeLock();
        try {
            DAssert.isTrue(open, ErrorType.DB_CLOSE, "db is closed");
        } catch (KitDBException e) {
            lock.unlock();
            throw e;
        }
        return new CloseLock(lock);
    }





    protected DBAbs() {

    }

    public void commitTX() throws KitDBException {
        try {
            DAssert.isTrue(this.IS_STATR_TX.get(), ErrorType.TX_NOT_START, "Transaction have not started");
            TransactionEntity entity = TRANSACTION_ENTITY.get();

            if (entity.getCount() > 0) {
                //事务不需要提交，计数器减一
                entity.subCount();
            } else {
                try {
                    DBCommandChunk dbCommandChunk = new DBCommandChunk(DBCommandChunkType.TX_COMMIT, entity);
                    functionCommit.call(dbCommandChunk);
                } finally {
                    IS_STATR_TX.set(false);
                    TX_LOCK.unlock();
                    entity.reset();
                }
            }
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }

    public void commitTX(TransactionEntity entity) throws RocksDBException {
        entity.getTransaction().commit();
    }


    public void rollbackTX() throws KitDBException {
        try {
            if (!this.IS_STATR_TX.get()) {
                return;
            }
            TransactionEntity entity = TRANSACTION_ENTITY.get();
            if (entity.getCount() > 0) {
                //事务不需要提交，计数器减一
                entity.subCount();
            } else {
                try {
                    DBCommandChunk dbCommandChunk = new DBCommandChunk(DBCommandChunkType.TX_ROLLBACK, entity);
                    functionCommit.call(dbCommandChunk);
                    //rollbackTX(entity);
                } finally {
                    IS_STATR_TX.set(false);
                    TX_LOCK.unlock();
                    entity.reset();
                }
            }
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }

    }

    public void rollbackTX(TransactionEntity entity) throws RocksDBException {
        entity.getTransaction().rollback();
    }

    public void start() {
        List<DBCommand> logs = threadLogs.get();
        if (logs == null) {
            logs = new ArrayList<>();
            threadLogs.set(logs);
        }
        logs.clear();
    }


    public void startTran(int waitTime) throws KitDBException {
        DAssert.isTrue(this.openTransaction, ErrorType.NOT_TX_DB, "this db is not a Transaction DB");
        if (!this.IS_STATR_TX.get()) {
            try {
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
            } catch (InterruptedException e) {
                throw new KitDBException(ErrorType.TX_GET_TIMEOUT, e);
            }
        } else {
            TRANSACTION_ENTITY.get().addCount();
        }
    }

    public void checkKey() throws KitDBException {
        DAssert.isTrue(this.openTransaction, ErrorType.NOT_TX_DB, "this db is not a Transaction DB");
        DAssert.isTrue(!TX_LOCK.isLocked(), ErrorType.TX_ERROR, "a Transaction is being executed");
    }


    public void commit(List<DBCommand> logs) throws KitDBException {
        try {
            if (this.IS_STATR_TX.get()) {
                Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
                try (final WriteBatch batch = new WriteBatch()) {
                    setLogs(logs, batch);
                    transaction.rebuildFromWriteBatch(batch);
                }
            } else {
                simpleCommit(logs);
            }
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }

    public void simpleCommit(List<DBCommand> logs) throws KitDBException, RocksDBException {
        try (final WriteBatch batch = new WriteBatch()) {
            setLogs(logs, batch);
            this.rocksDB().write(this.writeOptions(), batch);
        }
    }

    protected void commitLocal() throws KitDBException {
        try {
            List<DBCommand> logs = threadLogs.get();
            try {
                simpleCommit(logs);
            } finally {
                logs.clear();
            }
        } catch (Exception e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }


    protected void commit() throws KitDBException {
        try {
            DBCommandChunk dbCommandChunk = new DBCommandChunk();

            if (this.IS_STATR_TX.get()) {
                dbCommandChunk.setType(DBCommandChunkType.TX_LOGS);
            } else {
                dbCommandChunk.setType(DBCommandChunkType.NOM_COMMIT);
            }
            List<DBCommand> logs = threadLogs.get();
            dbCommandChunk.setCommands(logs);
            try {
                functionCommit.call(dbCommandChunk);
            } finally {
                logs.clear();
            }
        } catch (Exception e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
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


    public void simplePut(byte[] key, byte[] value, SstColumnFamily columnFamily) throws KitDBException {
        List<DBCommand> logs = new ArrayList<>(1);
        logs.add(DBCommand.update(key, value, columnFamily));
        DBCommandChunk dbCommandChunk = new DBCommandChunk(DBCommandChunkType.SIMPLE_COMMIT, logs);

        try {
            try {
                simpleCommit(logs);
            } finally {
                logs.clear();
            }
        } catch (Exception e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
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


    private void setLogs(List<DBCommand> logs, WriteBatch batch) throws KitDBException {
        try {
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
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }

    protected final List<ColumnFamilyOptions> cfOptionsList = new ArrayList<>();


    protected List<ColumnFamilyDescriptor> getColumnFamilyDescriptor() {
        final ColumnFamilyOptions cfOptions = TableConfig.createColumnFamilyOptions();
        final ColumnFamilyOptions defCfOptions = TableConfig.createDefColumnFamilyOptions();
        cfOptionsList.add(cfOptions);
        cfOptionsList.add(defCfOptions);

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


    protected byte[] getDB(byte[] key, SstColumnFamily columnFamily) throws KitDBException {
        try {
            if (this.IS_STATR_TX.get()) {
                Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
                return transaction.get(findColumnFamilyHandle(columnFamily), readOptions, key);
            }
            return this.rocksDB().get(findColumnFamilyHandle(columnFamily), key);
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }

    public byte[] simpleGet(byte[] key, SstColumnFamily columnFamily) throws KitDBException {
        try {
            return this.rocksDB().get(findColumnFamilyHandle(columnFamily), key);
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }


    protected RocksIterator newIterator(SstColumnFamily columnFamily) {

        if (this.IS_STATR_TX.get()) {
            Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
            return transaction.getIterator(readOptions, findColumnFamilyHandle(columnFamily));
        }

        return this.rocksDB().newIterator(findColumnFamilyHandle(columnFamily));
    }

    private static int computeCapacityHint(final int estimatedNumberOfItems) {
        // Default load factor for HashMap is 0.75, so N * 1.5 will be at the load
        // limit. We add +1 for a buffer.
        return (int) Math.ceil(estimatedNumberOfItems * 1.5 + 1.0);
    }

    protected Map<byte[], byte[]> multiGet(List<byte[]> keys, SstColumnFamily columnFamily) throws KitDBException {

        try {
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
            for (byte[] ignored : keys) {
                columnFamilyHandles.add(findColumnFamilyHandle(columnFamily));
            }
            if (this.IS_STATR_TX.get()) {
                Transaction transaction = TRANSACTION_ENTITY.get().getTransaction();
                byte[][] keys_bytes = keys.toArray(new byte[keys.size()][]);
                byte[][] values = transaction.multiGet(readOptions, columnFamilyHandles, keys_bytes);

                final Map<byte[], byte[]> keyValueMap
                        = new HashMap<>(computeCapacityHint(values.length));
                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null) {
                        continue;
                    }
                    keyValueMap.put(keys.get(i), values[i]);
                }
                return keyValueMap;
            }
            return this.rocksDB().multiGet(columnFamilyHandles, keys);
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
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
                deleteDB(end, columnFamily);
            }
        } finally {
            iterator.close();
        }
    }


    public interface FunctionCommit {
        void call(DBCommandChunk dbCommandChunk) throws KitDBException, RocksDBException;
    }

    volatile public FunctionCommit functionCommit = (dbCommandChunk) -> {
        DBCommandChunkType dbCommandChunkType = dbCommandChunk.getType();
        switch (dbCommandChunkType) {
            case NOM_COMMIT:
                this.commit(dbCommandChunk.getCommands());
                break;
            case TX_LOGS:
                this.commit(dbCommandChunk.getCommands());
                break;
            case TX_COMMIT:
                this.commitTX(dbCommandChunk.getEntity());
                break;
            case TX_ROLLBACK:
                this.rollbackTX(dbCommandChunk.getEntity());
                break;
            case SIMPLE_COMMIT:
                this.simpleCommit(dbCommandChunk.getCommands());
                break;
            default:
                throw new KitDBException(ErrorType.NULL, "DBCommandChunkType non-existent!");
        }
    };
}
