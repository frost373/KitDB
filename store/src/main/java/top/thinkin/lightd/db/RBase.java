package top.thinkin.lightd.db;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.data.KeyEnum;

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
