
package top.thinkin.lightd.collect;

import org.rocksdb.RocksDBException;

import java.util.List;

public class RKv extends RBase {
    public final static String HEAD = "K";
    public final static byte[] HEAD_TTL = "T".getBytes();

    public final static byte[] HEAD_B = HEAD.getBytes();


    public RKv(DB db) {
        this.db = db;
    }

    public void set(String key, byte[] value) throws Exception {
        start();
        byte[] key_b = ArrayKits.addAll(HEAD_B, key.getBytes(charset));
        putDB(key_b, value);
        commit();
    }

    public void setTTL(String key, byte[] value, int ttl) throws Exception {
        start();
        byte[] key_b = ArrayKits.addAll(HEAD_B, key.getBytes(charset));
        putDB(ArrayKits.addAll(HEAD_B, key.getBytes(charset)), value);
        int time = (int) (System.currentTimeMillis() / 1000 + ttl);
        putDB(ArrayKits.addAll(HEAD_TTL, key.getBytes(charset)), ArrayKits.intToBytes(time));
        db.ttlZset().add(key_b, time);
        commit();
    }


    public void ttl(String key, int ttl) throws Exception {
        start();
        byte[] key_b = ArrayKits.addAll(HEAD_B, key.getBytes(charset));
        db.ttlZset().add(key_b, System.currentTimeMillis() / 1000 + ttl);
        commit();
    }

    public byte[] get(String key) throws RocksDBException {
        byte[] value_bs = db.rocksDB().get(ArrayKits.addAll(HEAD_TTL, key.getBytes(charset)));
        if (value_bs != null) {
            int time = ArrayKits.bytesToInt(value_bs, 0);
            if ((System.currentTimeMillis() / 1000) - time <= 0) {
                return null;
            }
        }
        return db.rocksDB().get(ArrayKits.addAll(HEAD_B, key.getBytes(charset)));
    }

    public byte[] getNoTTL(String key) throws RocksDBException {
        return db.rocksDB().get(ArrayKits.addAll(HEAD_B, key.getBytes(charset)));
    }

    public void del(String key) throws Exception {
        start();
        deleteDB(ArrayKits.addAll(HEAD_B, key.getBytes(charset)));
        deleteDB(ArrayKits.addAll(HEAD_TTL, key.getBytes(charset)));
        commit();
    }

    public void delPrefix(String key_) {

    }

    public void getPrefix(String key_) {

    }

    public List<byte[]> keys(String key_) {

        return null;
    }


    /**
     * 获取过期时间戳(秒)
     *
     * @return
     * @throws Exception <p>
     *                   删除过期时间
     * @throws Exception <p>
     *                   设置新的过期时间戳(秒)
     * @throws Exception
     */

    int getTtl() throws Exception {
        return -1;
    }

    /**
     * 删除过期时间
     *
     * @return
     * @throws Exception
     */

    void delTtl() throws Exception {

    }

    /**
     * 设置新的过期时间戳(秒)
     *
     * @return
     * @throws Exception
     */

    void ttl(int ttl) throws Exception {

    }


}

