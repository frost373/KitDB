
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

    public void set(byte[] key, byte[] value) throws Exception {
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            putDB(key_b, value);
            commit();
        } finally {
            release();
        }
    }

    public void setTTL(byte[] key, byte[] value, int ttl) throws Exception {
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            putDB(ArrayKits.addAll(HEAD_B, key), value);
            int time = (int) (System.currentTimeMillis() / 1000 + ttl);
            putDB(ArrayKits.addAll(HEAD_TTL, key), ArrayKits.intToBytes(time));
            db.ttlZset().add(key_b, time);
            commit();
        } finally {
            release();
        }
    }


    public void ttl(byte[] key, int ttl) throws Exception {
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            db.ttlZset().add(key_b, System.currentTimeMillis() / 1000 + ttl);
            commit();
        } finally {
            release();
        }
    }

    public byte[] get(byte[] key) throws RocksDBException {
        byte[] value_bs = db.rocksDB().get(ArrayKits.addAll(HEAD_TTL, key));
        if (value_bs != null) {
            int time = ArrayKits.bytesToInt(value_bs, 0);
            if ((System.currentTimeMillis() / 1000) - time <= 0) {
                return null;
            }
        }
        return db.rocksDB().get(ArrayKits.addAll(HEAD_B, key));
    }

    public byte[] getNoTTL(byte[] key) throws RocksDBException {
        return db.rocksDB().get(ArrayKits.addAll(HEAD_B, key));
    }

    public void del(byte[] key) throws Exception {
        try {
            start();
            deleteDB(ArrayKits.addAll(HEAD_B, key));
            deleteDB(ArrayKits.addAll(HEAD_TTL, key));
            commit();
        } finally {
            release();
        }
    }

    public void delPrefix(byte[] key_) {

    }

    public void getPrefix(byte[] key_) {

    }

    public List<byte[]> keys(byte[] key_) {

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

