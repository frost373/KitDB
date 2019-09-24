package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.kit.ArrayKits;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

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
