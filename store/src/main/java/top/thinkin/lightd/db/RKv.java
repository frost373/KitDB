
package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.SegmentStrLock;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RKv extends RBase {
    public final static String HEAD = KeyEnum.KV_KEY.getKey();
    public final static byte[] HEAD_TTL = KeyEnum.KV_TTL.getBytes();

    public final static byte[] HEAD_B = HEAD.getBytes();
    private SegmentStrLock lock = new SegmentStrLock(128);


    protected RKv(DB db) {
        this.db = db;
    }

    public void set(String key, byte[] value) throws Exception {
        byte[] keyb = getKey(key);
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, keyb);
            putDB(key_b, value, SstColumnFamily.DEFAULT);
            deleteDB(ArrayKits.addAll(HEAD_TTL, keyb), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    protected byte[] getKey(String key) {
        DAssert.notNull(key, ErrorType.NULL, "Key is null");
        return key.getBytes(charset);
    }

    public long incr(String key, int step, int ttl) throws Exception {
        byte[] keyb = getKey(key);
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, keyb);

            byte[] value = get(key);
            long seq;
            if (value == null) {
                seq = step;
            } else {
                DAssert.isTrue(value.length == 8, ErrorType.DATA_LOCK, "value not a incr");
                seq = ArrayKits.bytesToLong(value) + step;
            }

            putDB(key_b, ArrayKits.longToBytes(seq), SstColumnFamily.DEFAULT);
            int time = (int) (System.currentTimeMillis() / 1000 + ttl);
            putDB(ArrayKits.addAll(HEAD_TTL, keyb), ArrayKits.intToBytes(time), SstColumnFamily.DEFAULT);

            setTimer(time, KeyEnum.KV_TIMER, key_b);

            commit();
            return seq;
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public long incr(String key, int step) throws Exception {
        byte[] keyb = getKey(key);
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, keyb);

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

    public void set(Map<String, byte[]> map) throws Exception {
        try {
            start();

            for (Map.Entry<String, byte[]> entry : map.entrySet()) {
                lock.lock(entry.getKey());
                try {
                    byte[] key_b = ArrayKits.addAll(HEAD_B, getKey(entry.getKey()));
                    putDB(key_b, entry.getValue(), SstColumnFamily.DEFAULT);
                    deleteDB(ArrayKits.addAll(HEAD_TTL, getKey(entry.getKey())), SstColumnFamily.DEFAULT);
                } finally {
                    lock.unlock(entry.getKey());
                }
            }
            commit();
        } finally {
            release();
        }
    }

    public void set(Map<String, byte[]> map, int ttl) throws Exception {
        int time = (int) (System.currentTimeMillis() / 1000 + ttl);
        try {
            start();
            int i = 0;
            for (Map.Entry<String, byte[]> entry : map.entrySet()) {
                lock.lock(entry.getKey());
                try {
                    byte[] key_b = ArrayKits.addAll(HEAD_B, getKey(entry.getKey()));
                    putDB(key_b, entry.getValue(), SstColumnFamily.DEFAULT);
                    putDB(ArrayKits.addAll(HEAD_TTL, getKey(entry.getKey())), ArrayKits.intToBytes(time), SstColumnFamily.DEFAULT);
                    setTimer(time, KeyEnum.KV_TIMER, key_b);
                    i++;
                } finally {
                    lock.unlock(entry.getKey());
                }
            }
            commit();
        } finally {
            release();
        }
    }

    public void set(String key, byte[] value, int ttl) throws Exception {
        byte[] keyb = getKey(key);
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, keyb);
            putDB(key_b, value, SstColumnFamily.DEFAULT);
            int time = (int) (System.currentTimeMillis() / 1000) + ttl;
            putDB(ArrayKits.addAll(HEAD_TTL, keyb), ArrayKits.intToBytes(time), SstColumnFamily.DEFAULT);
            setTimer(time, KeyEnum.KV_TIMER, key_b);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    public void ttl(String key, int ttl) throws Exception {
        byte[] keyb = getKey(key);
        lock.lock(key);
        try {
            start();
            byte[] key_b = ArrayKits.addAll(HEAD_B, keyb);
            int time = (int) (System.currentTimeMillis() / 1000) + ttl;
            putDB(ArrayKits.addAll(HEAD_TTL, keyb), ArrayKits.intToBytes(time), SstColumnFamily.DEFAULT);
            setTimer(time, KeyEnum.KV_TIMER, key_b);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public Map<String, byte[]> get(List<String> keys) throws RocksDBException {
        DAssert.notEmpty(keys, ErrorType.EMPTY, "keys is empty");

        byte[][] keybs = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            keybs[i] = getKey(keys.get(i));
        }

        List<byte[]> vKeys = new ArrayList<>(keybs.length);

        for (byte[] key : keybs) {
            vKeys.add(ArrayKits.addAll(HEAD_TTL, key));
            vKeys.add(ArrayKits.addAll(HEAD_B, key));
        }
        vKeys.addAll(vKeys);
        Map<String, byte[]> resMap = transMap(multiGet(vKeys, SstColumnFamily.DEFAULT));
        Map<String, byte[]> map = new HashMap<>(keybs.length);

        for (byte[] key : keybs) {
            byte[] ttl_bs = resMap.get(new String(ArrayKits.addAll(HEAD_TTL, key)));
            if (ttl_bs == null) {
                map.put(new String(key, charset), resMap.get(new String((ArrayKits.addAll(HEAD_B, key)))));
            } else {
                int time = ArrayKits.bytesToInt(ttl_bs, 0);
                if ((System.currentTimeMillis() / 1000) - time <= 0) {
                    map.put(new String(key, charset), null);
                } else {
                    map.put(new String(key, charset), resMap.get(new String((ArrayKits.addAll(HEAD_B, key)))));
                }
            }

        }

        return map;
    }


    private Map<String, byte[]> transMap(Map<byte[], byte[]> bsMap) {
        Map<String, byte[]> map = new HashMap<>(bsMap.size());

        for (Map.Entry<byte[], byte[]> entry : bsMap.entrySet()) {
            map.put(new String(entry.getKey()), entry.getValue());
        }

        return map;
    }


    protected void delCheckTTL(String key, int ztime) throws Exception {
        lock.lock(key);
        byte[] keyb = getKey(key);
        try {
            List<byte[]> keys = new ArrayList<>();
            keys.add(ArrayKits.addAll(HEAD_TTL, keyb));
            keys.add(ArrayKits.addAll(HEAD_B, keyb));

            Map<String, byte[]> resMap = transMap(multiGet(keys, SstColumnFamily.DEFAULT));
            byte[] ttl_bs = resMap.get(new String(ArrayKits.addAll(HEAD_TTL, keyb)));
            if (ttl_bs == null) {
                return;
            }
            int time = ArrayKits.bytesToInt(ttl_bs, 0);

            if (ztime < time) {
                return;
            }

            start();
            deleteDB(ArrayKits.addAll(HEAD_B, keyb), SstColumnFamily.DEFAULT);
            deleteDB(ArrayKits.addAll(HEAD_TTL, keyb), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public byte[] get(String key) throws RocksDBException {
        byte[] keyb = getKey(key);
        List<byte[]> keys = new ArrayList<>();
        keys.add(ArrayKits.addAll(HEAD_TTL, keyb));
        keys.add(ArrayKits.addAll(HEAD_B, keyb));

        Map<String, byte[]> resMap = transMap(multiGet(keys, SstColumnFamily.DEFAULT));
        byte[] ttl_bs = resMap.get(new String(ArrayKits.addAll(HEAD_TTL, keyb)));
        if (ttl_bs == null) {
            return resMap.get(new String(ArrayKits.addAll(HEAD_B, keyb)));
        }
        int time = ArrayKits.bytesToInt(ttl_bs, 0);
        if ((System.currentTimeMillis() / 1000) - time >= 0) {
            return null;
        } else {
            return resMap.get(new String(ArrayKits.addAll(HEAD_B, keyb)));
        }
    }

    public byte[] getNoTTL(String key) throws RocksDBException {
        byte[] keyb = getKey(key);

        return getDB(ArrayKits.addAll(HEAD_B, keyb), SstColumnFamily.DEFAULT);
    }

    public void del(String key) throws Exception {
        byte[] keyb = getKey(key);
        lock.lock(key);
        try {
            start();
            deleteDB(ArrayKits.addAll(HEAD_B, keyb), SstColumnFamily.DEFAULT);
            deleteDB(ArrayKits.addAll(HEAD_TTL, keyb), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public void delPrefix(String key_) throws Exception {
        byte[] keyb_ = getKey(key_);
        try {
            start();
            deleteHead(ArrayKits.addAll(HEAD_B, keyb_), SstColumnFamily.DEFAULT);
            deleteHead(ArrayKits.addAll(HEAD_TTL, keyb_), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            release();
        }
    }


    public List<String> keys(String key_, int start, int limit) {
        byte[] keyb_ = getKey(key_);
        List<String> list = new ArrayList<>();
        int index = 0;
        int count = 0;
        try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
            byte[] head = ArrayKits.addAll(HEAD_B, keyb_);
            iterator.seek(head);
            while (iterator.isValid() && count < limit) {
                byte[] key = iterator.key();
                if (!BytesUtil.checkHead(head, key)) break;
                if (index >= start) {
                    list.add(new String(ArrayUtil.sub(key, 1, key.length), charset));
                    count++;
                }
                index++;
                iterator.next();
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

    int getTtl(String key) throws Exception {
        byte[] keyb = getKey(key);
        byte[] value_bs = getDB(ArrayKits.addAll(HEAD_TTL, keyb), SstColumnFamily.DEFAULT);
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

    void delTtl(String key) throws Exception {
        lock.lock(key);
        byte[] keyb = getKey(key);

        try {
            start();
            deleteDB(ArrayKits.addAll(HEAD_TTL, keyb), SstColumnFamily.DEFAULT);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }


    @Data
    @AllArgsConstructor
    public static class Entry {
        private String key;
        private byte[] value;
    }









}

