package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.base.TxLock;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.LightDException;
import top.thinkin.lightd.kit.ArrayKits;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

@Slf4j

public abstract class RBase {

    protected DB db;

    protected final boolean isLog;

    protected static Charset charset = Charset.forName("UTF-8");


    public RBase(boolean isLog) {
        this.isLog = isLog;
    }

    public RBase() {
        this.isLog = false;
    }


    public void start() {
        db.start();
    }


    public void setTimer(KeyEnum keyEnum, int time, byte[] value) {

        TimerStore.put(this, keyEnum.getKey(), time, value);

    }


    public void setTimerCollection(KeyEnum keyEnum, int time, byte[] key_b, byte[] meta_b) {
        byte[] key_b_size_b = ArrayKits.intToBytes(key_b.length);
        TimerStore.put(this, keyEnum.getKey(), time, ArrayKits.addAll(key_b_size_b, key_b, meta_b));
    }


    public void delTimerCollection(KeyEnum keyEnum, int time, byte[] key_b, byte[] meta_b) {
        byte[] key_b_size_b = ArrayKits.intToBytes(key_b.length);
        TimerStore.del(this, keyEnum.getKey(), time, ArrayKits.addAll(key_b_size_b, key_b, meta_b));
    }

    public static class TimerCollection {
        public byte[] key_b;
        public byte[] meta_b;

    }


    protected void checkTxStart(String key) throws Exception {
        if (db.openTransaction) {
            TxLock lock = getTxLock(key);
            db.startTran(lock);
            try {
                DAssert.isTrue(db.TRANSACTION_ENTITY.get().checkKey(lock.getKey()),
                        ErrorType.TX_ERROR, lock.getKey() + " is not registered with the TX");
            } catch (LightDException e) {
                for (String lockKey : db.TRANSACTION_ENTITY.get().getLockKeys()) {
                    log.debug(lockKey);
                }
                log.error("error", e);
                db.rollbackTX();
                throw e;
            }
        }
    }

    protected void checkTxRange(String key) throws Exception {
        if (!db.openTransaction) {
            return;
        }
        DAssert.isTrue(!this.db.IS_STATR_TX.get(), ErrorType.TX_ERROR,
                "This operation can't execute  in a transaction");
        db.checkKey(getTxLock(key));
    }


    protected void checkTxStart(String... keys) throws Exception {

        if (db.openTransaction) {
            TxLock[] locks = new TxLock[keys.length];
            for (int i = 0; i < keys.length; i++) {
                locks[i] = getTxLock(keys[i]);
            }

            db.startTran(locks);

            try {

                for (TxLock lock : locks) {
                    DAssert.isTrue(db.TRANSACTION_ENTITY.get().checkKey(lock.getKey()),
                            ErrorType.TX_ERROR, lock.getKey() + " is not registered with the TX");
                }
            } catch (LightDException e) {
                db.rollbackTX();
                throw e;
            }
        }
    }


    protected void checkTxCommit() throws Exception {
        if (db.openTransaction) {
            db.commitTX();
        }
    }

    protected void checkTxRollBack() throws Exception {
        if (db.openTransaction) {
            db.rollbackTX();
        }
    }

    protected abstract TxLock getTxLock(String key);

    public static TimerCollection getTimerCollection(byte[] value) {
        byte[] key_b_size_b = ArrayUtil.sub(value, 0, 4);
        int size = ArrayKits.bytesToInt(key_b_size_b, 0);
        TimerCollection timerCollection = new TimerCollection();
        timerCollection.key_b = ArrayUtil.sub(value, 4, 4 + size);
        timerCollection.meta_b = ArrayUtil.sub(value, 4 + size, value.length);
        return timerCollection;
    }

    public void delTimer(KeyEnum keyEnum, int time, byte[] value) {
        TimerStore.del(this, keyEnum.getKey(), time, value);
    }


    public void commit() throws Exception {
        db.commit();
    }


    public void release() {
        db.release();
    }


    public void putDB(byte[] key, byte[] value, SstColumnFamily columnFamily) {
        db.putDB(key, value, columnFamily);
    }

    public void deleteDB(byte[] key, SstColumnFamily columnFamily) {
        db.deleteDB(key, columnFamily);
    }


    protected void deleteRangeDB(byte[] start, byte[] end, SstColumnFamily columnFamily) {
        db.deleteRangeDB(start, end, columnFamily);
    }


    protected byte[] getDB(byte[] key, SstColumnFamily columnFamily) throws RocksDBException {
        return db.getDB(key, columnFamily);
    }


    protected RocksIterator newIterator(SstColumnFamily columnFamily) {
        return db.newIterator(columnFamily);
    }


    protected Map<byte[], byte[]> multiGet(List<byte[]> keys, SstColumnFamily columnFamily) throws RocksDBException {
        return db.multiGet(keys, columnFamily);
    }


    protected void deleteHead(byte[] head, SstColumnFamily columnFamily) {
        db.deleteHead(head, columnFamily);
    }


}
