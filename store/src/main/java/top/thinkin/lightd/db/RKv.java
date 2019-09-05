
package top.thinkin.lightd.db;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.SegmentLock;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.data.ReservedWords;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.kit.ArrayKits;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RKv extends RBase {
    public final static String HEAD = KeyEnum.KV_KEY.getKey();
    public final static byte[] HEAD_TTL = KeyEnum.KV_TTL.getBytes();

    public final static byte[] HEAD_B = HEAD.getBytes();
    private SegmentLock lock = new SegmentLock(128);

    protected RKv(DB db) {
        this.db = db;
    }

    public void set(byte[] key, byte[] value) throws Exception {
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            putDB(key_b, value, SstColumnFamily.DEFAULT);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public long incr(byte[] key, int step, int ttl) throws Exception {
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);

            byte[] value = get(key_b);
            long seq;
            if (value == null) {
                seq = step;
            } else {
                DAssert.isTrue(value.length == 8, ErrorType.DATA_LOCK, "value not a incr");
                seq = ArrayKits.bytesToLong(value) + step;
            }

            putDB(key_b, ArrayKits.longToBytes(seq), SstColumnFamily.DEFAULT);
            int time = (int) (System.currentTimeMillis() / 1000 + ttl);
            putDB(ArrayKits.addAll(HEAD_TTL, key), ArrayKits.intToBytes(time), SstColumnFamily.DEFAULT);
            db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, key_b, time);

            commit();
            return seq;
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public long incr(byte[] key, int step) throws Exception {
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);

            byte[] value = get(key);
            long seq;
            if (value == null) {
                seq = step;
            } else {
                DAssert.isTrue(value.length == 8, ErrorType.DATA_LOCK, "value not a incr");
                seq = ArrayKits.bytesToLong(value) + step;
            }
            putDB(key_b, ArrayKits.longToBytes(seq), SstColumnFamily.DEFAULT);
            commit();
            return seq;
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
                    putDB(key_b, kv.value, SstColumnFamily.DEFAULT);
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
                    putDB(key_b, kvs.get(i).value, SstColumnFamily.DEFAULT);
                    entrys[i] = new ZSet.Entry(time, key_b);
                } finally {
                    lock.unlock(kvs.get(i).key);
                }
            }
            commit();
            db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, entrys);
        } finally {
            release();
        }
    }

    public void setTTL(byte[] key, byte[] value, int ttl) throws Exception {
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, key);
            putDB(ArrayKits.addAll(HEAD_B, key), value, SstColumnFamily.DEFAULT);
            int time = (int) (System.currentTimeMillis() / 1000) + ttl;
            putDB(ArrayKits.addAll(HEAD_TTL, key), ArrayKits.intToBytes(time), SstColumnFamily.DEFAULT);
            commit();
            db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, key_b, time);
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
            commit();
            db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, key_b, System.currentTimeMillis() / 1000 + ttl);
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public Map<byte[], byte[]> get(byte[]... keys) throws RocksDBException {
        DAssert.notEmpty(keys, ErrorType.EMPTY, "keys is empty");
        List<byte[]> vKeys = new ArrayList<>(keys.length);

        for (byte[] key : keys) {
            vKeys.add(ArrayKits.addAll(HEAD_TTL, key));
            vKeys.add(ArrayKits.addAll(HEAD_B, key));
        }
        vKeys.addAll(vKeys);
        Map<String, byte[]> resMap = transMap(multiGet(vKeys, SstColumnFamily.DEFAULT));
        Map<byte[], byte[]> map = new HashMap<>(keys.length);

        for (byte[] key : keys) {
            byte[] ttl_bs = resMap.get(new String(ArrayKits.addAll(HEAD_TTL, key)));
            if (ttl_bs == null) {
                map.put(key, resMap.get(new String((ArrayKits.addAll(HEAD_B, key)))));
            }
            int time = ArrayKits.bytesToInt(ttl_bs, 0);
            if ((System.currentTimeMillis() / 1000) - time <= 0) {
                map.put(key, null);
            } else {
                map.put(key, resMap.get(new String((ArrayKits.addAll(HEAD_B, key)))));
            }
        }

        return map;
    }


    public Map<String, byte[]> transMap(Map<byte[], byte[]> bsMap) {
        Map<String, byte[]> map = new HashMap<>(bsMap.size());

        for (Map.Entry<byte[], byte[]> entry : bsMap.entrySet()) {
            map.put(new String(entry.getKey()), entry.getValue());
        }

        return map;
    }


    public byte[] get(byte[] key) throws RocksDBException {
        List<byte[]> keys = new ArrayList<>();
        keys.add(ArrayKits.addAll(HEAD_TTL, key));
        keys.add(ArrayKits.addAll(HEAD_B, key));

        Map<String, byte[]> resMap = transMap(multiGet(keys, SstColumnFamily.DEFAULT));
        byte[] ttl_bs = resMap.get(new String(ArrayKits.addAll(HEAD_TTL, key)));
        if (ttl_bs == null) {
            return resMap.get(new String(ArrayKits.addAll(HEAD_B, key)));
        }
        int time = ArrayKits.bytesToInt(ttl_bs, 0);
        if ((System.currentTimeMillis() / 1000) - time >= 0) {
            return null;
        } else {
            return resMap.get(new String(ArrayKits.addAll(HEAD_B, key)));
        }
    }

    public byte[] getNoTTL(byte[] key) throws RocksDBException {
        return getDB(ArrayKits.addAll(HEAD_B, key), SstColumnFamily.DEFAULT);
    }

    public void del(byte[] key) throws Exception {
        lock.lock(key);
        try {
            start();
            deleteDB(ArrayKits.addAll(HEAD_B, key), SstColumnFamily.DEFAULT);
            deleteDB(ArrayKits.addAll(HEAD_TTL, key), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    public void release(byte[] key) {
        lock.lock(key);
        try {
            byte[] value_bs = getDB(ArrayKits.addAll(HEAD_TTL, key), SstColumnFamily.DEFAULT);
            if (value_bs != null) {
                int time = ArrayKits.bytesToInt(value_bs, 0);
                if ((System.currentTimeMillis() / 1000) - time <= 0) {
                    try {
                        start();
                        deleteDB(ArrayKits.addAll(HEAD_B, key), SstColumnFamily.DEFAULT);
                        deleteDB(ArrayKits.addAll(HEAD_TTL, key), SstColumnFamily.DEFAULT);
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

    public void delPrefix(byte[] key_) throws Exception {
        try {
            start();
            deleteHead(ArrayKits.addAll(HEAD_B, key_), SstColumnFamily.DEFAULT);
            deleteHead(ArrayKits.addAll(HEAD_TTL, key_), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            release();
        }
    }


    public List<byte[]> keys(byte[] key_, int start, int limit) {
        List<byte[]> list = new ArrayList<>();
        int index = 0;
        int count = 0;
        try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
            iterator.seek(ArrayKits.addAll(HEAD_B, key_));
            while (iterator.isValid() && count <= limit) {
                byte[] key = iterator.key();
                index++;
                if (index >= start) {
                    list.add(key);
                    count++;
                }
            }
        }
        return list;
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
        byte[] value_bs = getDB(ArrayKits.addAll(HEAD_TTL, key), SstColumnFamily.DEFAULT);
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
            deleteDB(ArrayKits.addAll(HEAD_TTL, key), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            release();
        }
    }




}

