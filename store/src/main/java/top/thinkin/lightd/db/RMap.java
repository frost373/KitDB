package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.MetaAbs;
import top.thinkin.lightd.base.MetaDAbs;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.data.ReservedWords;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.kit.ArrayKits;

import java.util.*;

public class RMap extends RCollection {
    public final static String HEAD = KeyEnum.MAP.getKey();
    public static byte[] HEAD_B = HEAD.getBytes();
    public final static byte[] HEAD_KEY_B = KeyEnum.MAP_KEY.getBytes();

    public RMap(DB db) {
        this.db = db;
    }

    protected byte[] getKey(String key) {
        DAssert.notNull(key, ErrorType.NULL, "Key is null");
        return ArrayKits.addAll(HEAD_B, key.getBytes(charset));
    }


    private Meta addCheck(byte[] key_b, byte[] k_v) throws RocksDBException {
        Meta metaV = null;
        if (k_v != null) {
            MetaD metaVD = MetaD.build(k_v);
            metaV = metaVD.convertMeta();
            long nowTime = System.currentTimeMillis() / 1000;
            if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
                metaV = null;
                db.rocksDB().put(ArrayKits.addAll("D".getBytes(), key_b, metaVD.getVersion()), metaVD.toBytes());
            }
        }
        return metaV;
    }


    private void setEntry(byte[] key_b, Meta metaV, Entry[] entrys) {
        for (Entry entry : entrys) {
            metaV.size = metaV.size + 1;
            Key key = new Key(key_b.length, key_b, metaV.getVersion(), entry.key);
            putDB(key.convertBytes().toBytes(), entry.value);
        }
    }

    public synchronized void put(String key, byte[] mkey, byte[] value) throws Exception {
        putTTL(key, mkey, value, -1);
    }

    public synchronized void putTTL(String key, byte[] mkey, byte[] value, int ttl) throws Exception {
        byte[] key_b = getKey(key);
        start();
        try {
            byte[] k_v = db.rocksDB().get(key_b);
            Meta metaV = addCheck(key_b, k_v);

            if (ttl != -1) {
                ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
            }

            if (metaV != null) {
                metaV.size = metaV.size + 1;
                metaV.setTimestamp(ttl);
                Key key_ = new Key(key_b.length, key_b, metaV.getVersion(), mkey);
                putDB(key_.convertBytes().toBytes(), value);
                putDB(key_b, metaV.convertMetaBytes().toBytes());
            } else {

                metaV = new Meta(0, ttl, db.versionSequence().incr());
                metaV.size = metaV.size + 1;
                Key key_ = new Key(key_b.length, key_b, metaV.getVersion(), mkey);
                putDB(key_.convertBytes().toBytes(), value);
                putDB(key_b, metaV.convertMetaBytes().toBytes());
            }

            if (metaV.getTimestamp() != -1) {
                db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }

            commit();
        } finally {
            release();
        }
    }


    public synchronized void putMayTTL(String key, int ttl, Entry... entries) throws Exception {
        byte[] key_b = getKey(key);

        DAssert.notEmpty(entries, ErrorType.EMPTY, "entries is empty");
        byte[][] bytess = new byte[entries.length][];
        for (int i = 0; i < entries.length; i++) {
            bytess[i] = entries[i].value;
        }
        DAssert.isTrue(ArrayKits.noRepeate(bytess), ErrorType.REPEATED_KEY, "Repeated keys");
        start();
        try {
            byte[] k_v = db.rocksDB().get(key_b);
            Meta metaV = addCheck(key_b, k_v);

            if (ttl != -1) {
                ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
            }

            if (metaV != null) {
                setEntry(key_b, metaV, entries);
                metaV.setTimestamp(ttl);
                putDB(key_b, metaV.convertMetaBytes().toBytes());
            } else {

                metaV = new Meta(0, ttl, db.versionSequence().incr());
                setEntry(key_b, metaV, entries);
                putDB(key_b, metaV.convertMetaBytes().toBytes());
            }

            if (metaV.getTimestamp() != -1) {
                db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }
            commit();
        } finally {
            release();
        }
    }


    public Map<byte[], byte[]> get(String key, byte[]... keys) throws Exception {
        byte[] key_b = getKey(key);
        DAssert.notEmpty(keys, ErrorType.EMPTY, "keys is empty");
        Meta metaV = getMeta(key_b);
        if (metaV == null) {
            return new HashMap<>();
        }

        Map<byte[], byte[]> map = new HashMap<>(keys.length);
        List<byte[]> keyList = new ArrayList<>();
        for (byte[] mkey : keys) {
            Key vkey = new Key(key_b.length, key_b, metaV.getVersion(), mkey);
            keyList.add(vkey.convertBytes().toBytes());
        }
        Map<byte[], byte[]> resMap = db.rocksDB().multiGet(keyList);
        Iterator<Map.Entry<byte[], byte[]>> iter = resMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iter.next();
            byte[] resKey = entry.getKey();
            KeyD keyd = KeyD.build(resKey);
            map.put(keyd.convertValue().getKey(), entry.getValue());
        }
        return map;
    }


    public byte[] get(String key, byte[] mkey) throws Exception {
        byte[] key_b = getKey(key);
        Meta metaV = getMeta(key_b);
        if (metaV == null) {
            return null;
        }
        Key vkey = new Key(key_b.length, key_b, metaV.getVersion(), mkey);
        byte[] value = db.rocksDB().get(vkey.convertBytes().toBytes());
        return value;
    }


    protected Meta getMeta(byte[] key_b) throws Exception {
        byte[] k_v = this.db.rocksDB().get(key_b);
        if (k_v == null) {
            return null;
        }
        Meta metaV = MetaD.build(k_v).convertMeta();
        long nowTime = System.currentTimeMillis();
        if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
            throw new Exception("List do not exist");
        }
        return metaV;
    }


    public synchronized void remove(String key, byte[]... keys) throws Exception {
        DAssert.notEmpty(keys, ErrorType.EMPTY, "keys is empty");
        byte[] key_b = getKey(key);

        Meta metaV = getMeta(key_b);
        if (metaV == null) {
            return;
        }

        try {
            start();
            for (byte[] mkey : keys) {
                Key vkey = new Key(key_b.length, key_b, metaV.getVersion(), mkey);

                byte[] value = db.rocksDB().get(vkey.convertBytes().toBytes());
                if (value != null) {
                    deleteDB(vkey.convertBytes().toBytes());
                    metaV.size = metaV.size - 1;
                }
            }
            putDB(key_b, metaV.convertMetaBytes().toBytes());
            commit();
        } finally {
            release();
        }
    }

    public static synchronized void delete(byte[] key_b, byte[] k_v, DB db) throws Exception {
        RList rBase = new RList(db);
        rBase.start();
        try {
            Meta meta = MetaD.build(k_v).convertMeta();
            delete(key_b, rBase, meta);
            rBase.commit();
        } finally {
            rBase.release();
        }
    }


    private static void delete(byte[] key_b, RBase rBase, Meta meta) {
        MetaD metaD = meta.convertMetaBytes();
        rBase.deleteDB(key_b);
        Key vkey = new Key(key_b.length, key_b, meta.getVersion(), null);
        deleteHead(vkey.getHead(), rBase);
        rBase.deleteDB(ArrayKits.addAll("D".getBytes(charset), key_b, metaD.getVersion()));
    }

    @Override
    public void delete(String key) throws Exception {
        byte[] key_b = getKey(key);
        Meta meta = getMeta(key_b);
        if (meta == null) {
            return;
        }
        try {
            start();
            delete(key_b, this, meta);
        } finally {
            release();
        }
    }


    public synchronized void deleteFast(String key) throws Exception {
        byte[] key_b = getKey(key);
        Meta metaV = getMeta(key_b);
        if (metaV == null) {
            return;
        }
        deleteFast(key_b, this, metaV);
    }

    @Override
    <T extends RCollection> RIterator<T> iterator(String key) throws Exception {
        return null;
    }




    @Override
    public int getTtl(String key) throws Exception {
        byte[] key_b = getKey(key);

        Meta meta = getMeta(key_b);
        if (meta == null) {
            return 0;
        }
        if (meta.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - meta.getTimestamp());
    }

    @Override
    public void delTtl(String key) throws Exception {
        byte[] key_b = getKey(key);

        Meta metaV = getMeta(key_b);
        if (metaV == null) {
            return;
        }
        try {
            metaV.setTimestamp(-1);
            start();
            putDB(key_b, metaV.convertMetaBytes().toBytes());

            db.getzSet().remove(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes());
            commit();
        } finally {
            release();
        }
    }

    @Override
    public void ttl(String key, int ttl) throws Exception {
        byte[] key_b = getKey(key);

        try {
            Meta metaV = getMeta(key_b);
            if (metaV == null) {
                metaV = new Meta(0, -1, db.versionSequence().incr());
            }
            start();
            if (ttl != -1) {
                metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
            }
            putDB(key_b, metaV.convertMetaBytes().toBytes());
            db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            commit();
        } finally {
            release();
        }
    }

    @Override
    public boolean isExist(String key) throws RocksDBException {
        byte[] key_b = getKey(key);

        byte[] k_v = db.rocksDB().get(key_b);
        Meta meta = addCheck(key_b, k_v);
        return meta != null;
    }

    @Override
    public int size(String key) throws Exception {
        byte[] key_b = getKey(key);
        Meta metaV = getMeta(key_b);
        if (metaV == null) {
            return 0;
        }
        return metaV.getSize();
    }

    @Override
    public Entry getEntry(RocksIterator iterator) {
        return null;
    }

    @Data
    @AllArgsConstructor
    public static class Entry extends RCollection.Entry {
        private byte[] key;
        private byte[] value;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Meta extends MetaAbs {
        private int size;
        private int timestamp;
        private int version;

        public MetaD convertMetaBytes() {
            MetaD metaVD = new MetaD();
            metaVD.setSize(ArrayKits.intToBytes(this.size));
            metaVD.setTimestamp(ArrayKits.intToBytes(this.timestamp));
            metaVD.setVersion(ArrayKits.intToBytes(this.version));
            return metaVD;
        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MetaD extends MetaDAbs {
        private byte[] size;
        private byte[] timestamp;
        private byte[] version;

        public static MetaD build(byte[] bytes) {
            MetaD metaD = new MetaD();
            metaD.setSize(ArrayUtil.sub(bytes, 1, 5));
            metaD.setTimestamp(ArrayUtil.sub(bytes, 5, 9));
            metaD.setVersion(ArrayUtil.sub(bytes, 9, 13));
            return metaD;
        }

        public byte[] toBytes() {
            byte[] value = ArrayKits.addAll(HEAD_B, this.size, this.timestamp, this.version);
            return value;
        }

        public Meta convertMeta() {
            Meta meta = new Meta();
            meta.setSize(ArrayKits.bytesToInt(this.size, 0));
            meta.setTimestamp(ArrayKits.bytesToInt(this.timestamp, 0));
            meta.setVersion(ArrayKits.bytesToInt(this.version, 0));
            return meta;
        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Key {
        private int mapKeySize;
        private byte[] mapKey;
        private int version;
        private byte[] key;

        public KeyD convertBytes() {
            KeyD keyD = new KeyD();
            keyD.setMapKeySize(ArrayKits.intToBytes(this.mapKeySize));
            keyD.setMapKey(this.mapKey);
            keyD.setVersion(ArrayKits.intToBytes(this.version));
            keyD.setKey(this.key);
            return keyD;
        }

        public byte[] getHead() {
            byte[] value = ArrayKits.addAll(HEAD_KEY_B, ArrayKits.intToBytes(this.mapKeySize),
                    this.mapKey, ArrayKits.intToBytes(this.version));
            return value;
        }
    }


    @Data
    public static class KeyD {
        private byte[] mapKeySize;
        private byte[] mapKey;
        private byte[] version;
        private byte[] key;

        public static KeyD build(byte[] bytes) {
            KeyD keyD = new KeyD();
            keyD.setMapKeySize(ArrayUtil.sub(bytes, 1, 5));
            int position = ArrayKits.bytesToInt(keyD.getMapKeySize(), 0);
            keyD.setMapKey(ArrayUtil.sub(bytes, 5, position = 5 + position));
            keyD.setVersion(ArrayUtil.sub(bytes, position, position + 4));
            keyD.setKey(ArrayUtil.sub(bytes, position, bytes.length - 1));
            return keyD;
        }

        public byte[] toBytes() {
            return ArrayKits.addAll(HEAD_KEY_B, this.mapKeySize, this.mapKey, this.version, this.key);
        }

        public Key convertValue() {
            Key key = new Key();
            key.setMapKeySize(ArrayKits.bytesToInt(this.mapKeySize, 0));
            key.setMapKey(this.mapKey);
            key.setVersion(ArrayKits.bytesToInt(this.version, 0));
            key.setKey(this.key);
            return key;
        }
    }
}
