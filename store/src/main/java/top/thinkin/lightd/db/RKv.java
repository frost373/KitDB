
package top.thinkin.lightd.db;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.base.SegmentLock;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.kit.ArrayKits;

import java.util.List;

public class RKv extends RBase {
    public final static String HEAD = KeyEnum.KV_KEY.getKey();
    public final static byte[] HEAD_TTL = KeyEnum.KV_TTL.getBytes();

    public final static byte[] HEAD_B = HEAD.getBytes();
    private SegmentLock lock = new SegmentLock(64);
    public RKv(DB db) {
        this.db = db;
    }

    public void set(byte[] key, byte[] value) throws Exception {
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            putDB(key_b, value);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    public void set(List<Entry> kvs) throws Exception {
        try {
            start();
            for (Entry kv : kvs) {
                lock.lock(kv.key);
                try {
                    byte[] key_b = ArrayKits.addAll(HEAD_B, kv.key);
                    putDB(key_b, kv.value);
                } finally {
                    lock.unlock(kv.key);
                }
            }
            commit();
        } finally {
            release();
        }
    }

    public void set(List<Entry> kvs, int ttl) throws Exception {
        int time = (int) (System.currentTimeMillis() / 1000 + ttl);
        try {
            start();
            ZSet.Entry[] entrys = new ZSet.Entry[kvs.size()];
            for (int i = 0; i < kvs.size(); i++) {
                lock.lock(kvs.get(i).key);
                try {
                    byte[] key_b = ArrayKits.addAll(HEAD_B, kvs.get(i).key);
                    putDB(key_b, kvs.get(i).value);
                    entrys[i] = new ZSet.Entry(time, key_b);
                } finally {
                    lock.unlock(kvs.get(i).key);
                }
            }
            db.ttlZset().add(entrys);
            commit();
        } finally {
            release();
        }
    }

    public void setTTL(byte[] key, byte[] value, int ttl) throws Exception {
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            putDB(ArrayKits.addAll(HEAD_B, key), value);
            int time = (int) (System.currentTimeMillis() / 1000 + ttl);
            putDB(ArrayKits.addAll(HEAD_TTL, key), ArrayKits.intToBytes(time));
            db.ttlZset().add(key_b, time);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    public void ttl(byte[] key, int ttl) throws Exception {
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            db.ttlZset().add(key_b, System.currentTimeMillis() / 1000 + ttl);
            commit();
        } finally {
            lock.unlock(key);
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
        lock.lock(key);
        try {
            start();
            deleteDB(ArrayKits.addAll(HEAD_B, key));
            deleteDB(ArrayKits.addAll(HEAD_TTL, key));
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    public void release(byte[] key) {
        lock.lock(key);
        try {
            byte[] value_bs = db.rocksDB().get(ArrayKits.addAll(HEAD_TTL, key));
            if (value_bs != null) {
                int time = ArrayKits.bytesToInt(value_bs, 0);
                if ((System.currentTimeMillis() / 1000) - time <= 0) {
                    try {
                        start();
                        deleteDB(ArrayKits.addAll(HEAD_B, key));
                        deleteDB(ArrayKits.addAll(HEAD_TTL, key));
                        commit();
                    } finally {
                        release();
                    }
                }
            }
        } catch (Exception e) {
            lock.unlock(key);
        }
    }

    @Data
    @AllArgsConstructor
    public static class Entry {
        private byte[] key;
        private byte[] value;
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

    int getTtl(byte[] key) throws Exception {
        byte[] value_bs = db.rocksDB().get(ArrayKits.addAll(HEAD_TTL, key));
        if (value_bs != null) {
            int time = ArrayKits.bytesToInt(value_bs, 0);
            int ttl = (int) ((System.currentTimeMillis() / 1000) - time);
            if (ttl <= 0) {
                return 0;
            }
            return ttl;
        }
        return -1;
    }

    /**
     * 删除过期时间
     *
     * @return
     * @throws Exception
     */

    void delTtl(byte[] key) throws Exception {
        try {
            start();
            deleteDB(ArrayKits.addAll(HEAD_TTL, key));
            commit();
        } finally {
            release();
        }
    }




}

