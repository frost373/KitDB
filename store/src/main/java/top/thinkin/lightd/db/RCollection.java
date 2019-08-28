package top.thinkin.lightd.db;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.MetaAbs;
import top.thinkin.lightd.base.MetaDAbs;
import top.thinkin.lightd.kit.ArrayKits;

public abstract class RCollection extends RBase {

    public synchronized void deleteFast(byte[] key_b) throws Exception {
        MetaAbs metaV = getMeta(key_b);
        deleteFast(key_b, this, metaV);
    }

    protected abstract <T extends MetaAbs> T getMeta(byte[] key_b) throws Exception;

    protected static void deleteFast(byte[] key_b, RBase rBase, MetaAbs metaV) throws Exception {
        rBase.start();
        try {
            MetaDAbs metaVD = metaV.convertMetaBytes();
            rBase.putDB(ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion()), metaVD.toBytes());
            rBase.deleteDB(key_b);
            rBase.commit();
        } finally {
            rBase.release();
        }
    }


    abstract <T extends RCollection> RIterator<T> iterator(String key) throws Exception;


    /**
     * 删除，数据会被同步清除
     *
     * @throws Exception
     */
    abstract void delete(String key) throws Exception;

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

    public static class Entry {

    }

    abstract Entry getEntry(RocksIterator iterator);


}
