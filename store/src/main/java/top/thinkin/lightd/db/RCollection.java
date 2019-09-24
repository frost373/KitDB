package top.thinkin.lightd.db;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.KeyDoubletLock;
import top.thinkin.lightd.base.MetaAbs;
import top.thinkin.lightd.base.MetaDAbs;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.kit.ArrayKits;

public abstract class RCollection extends RBase {
    protected final KeyDoubletLock lock;


    public RCollection(DB db, boolean isLog, int lockSize) {
        super(isLog);
        this.db = db;
        this.lock = db.getKeySegmentLockManager().createLock(2000);
    }


    protected abstract <T extends MetaAbs> T getMeta(byte[] key_b) throws Exception;

    protected void deleteFast(byte[] key_b, MetaAbs metaV) throws Exception {
        this.start();
        try {
            MetaDAbs metaVD = metaV.convertMetaBytes();
            this.putDB(ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion()), metaVD.toBytes(), SstColumnFamily.DEFAULT);
            this.deleteDB(key_b, SstColumnFamily.META);
            this.commit();
        } finally {
            this.release();
        }
    }


    protected KeyIterator getKeyIterator(byte[] head) {
        RocksIterator iterator = newIterator(SstColumnFamily.META);
        iterator.seek(head);
        KeyIterator keyIterator = new KeyIterator(iterator, head);
        return keyIterator;
    }

    abstract <T extends RCollection> RIterator<T> iterator(String key) throws Exception;


    /**
     * 删除，数据会被同步清除
     *
     * @throws Exception
     */
    abstract void delete(String key) throws Exception;

    public abstract KeyIterator getKeyIterator() throws Exception;

    /**
     * 获取过期时间戳(秒)
     * @return
     * @throws Exception
     */
    abstract int getTtl(String key) throws Exception;
    /**
     * 删除过期时间
     * @return
     * @throws Exception
     */
    abstract void delTtl(String key) throws Exception;
    /**
     * 设置新的过期时间戳(秒)
     * @return
     * @throws Exception
     */
    abstract void ttl(String key, int ttl) throws Exception;

    abstract boolean isExist(String key) throws RocksDBException;

    abstract int size(String key) throws Exception;


    abstract <E extends REntry> E getEntry(RocksIterator iterator);


}
