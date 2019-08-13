package top.thinkin.lightd.collect;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public interface RCollection {


    /**
     * 删除，数据会被同步清除
     * @throws Exception
     */
    void delete() throws Exception;

    /**
     * 快速删除,数据未被同步清理，但不影响新数据的创建和查询
     * @throws Exception
     */
    void  deleteFast() throws Exception;

    /**
     * 获取过期时间戳(秒)
     * @return
     * @throws Exception
     */
    int getTtl() throws Exception;
    /**
     * 删除过期时间
     * @return
     * @throws Exception
     */
    void delTtl() throws Exception;
    /**
     * 设置新的过期时间戳(秒)
     * @return
     * @throws Exception
     */
    void ttl(int timestamp) throws Exception;


    boolean isExist() throws RocksDBException;
    int size() throws Exception;
    class Entry  {

    }
    Entry getEntry(RocksIterator iterator);


}
