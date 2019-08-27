package top.thinkin.lightd.collect;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public abstract class RCollection extends RBase {

    public synchronized void deleteFast() throws Exception {
        MetaAbs metaV = getMeta();
        deleteFast(this.key_b, this, metaV);
    }

    protected abstract <T extends MetaAbs> T getMeta() throws Exception;

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


    abstract <T extends RCollection> RIterator<T> iterator() throws Exception;


    /**
     * 删除，数据会被同步清除
     *
     * @throws Exception
     */
    abstract void delete() throws Exception;

    /**
     * 获取过期时间戳(秒)
     * @return
     * @throws Exception
     */
    abstract int getTtl() throws Exception;
    /**
     * 删除过期时间
     * @return
     * @throws Exception
     */
    abstract void delTtl() throws Exception;
    /**
     * 设置新的过期时间戳(秒)
     * @return
     * @throws Exception
     */
    abstract void ttl(int ttl) throws Exception;

    abstract boolean isExist() throws RocksDBException;

    abstract int size() throws Exception;

    public static class Entry {

    }

    abstract Entry getEntry(RocksIterator iterator);


}
