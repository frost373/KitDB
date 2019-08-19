package top.thinkin.lightd.collect;

import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.*;

public class RMap extends RBase implements RCollection {
    public final static String HEAD = "M";
    public static byte[] HEAD_B = HEAD.getBytes();
    public final static byte[] HEAD_KEY_B = "m".getBytes();

    public RMap(DB db, String key) {
        this.key_b = ArrayKits.addAll(HEAD_B, key.getBytes(charset));
        this.db = db;
    }

    private Meta addCheck(byte[] k_v) throws RocksDBException {
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


    private void setEntry(Meta metaV, Entry[] entrys) {
        for (Entry entry : entrys) {
            metaV.size = metaV.size + 1;
            Key key = new Key(key_b.length, key_b, metaV.getVersion(), entry.key);
            putDB(key.convertBytes().toBytes(), entry.value);
        }
    }

    public synchronized void set(byte[] key, byte[] value) throws Exception {
        start();
        try {
            byte[] k_v = db.rocksDB().get(this.key_b);
            Meta metaV = addCheck(k_v);
            if (metaV != null) {
                metaV.size = metaV.size + 1;
                Key key_ = new Key(key_b.length, key_b, metaV.getVersion(), key);
                putDB(key_.convertBytes().toBytes(), value);
                putDB(this.key_b, metaV.convertMetaBytes().toBytes());
            } else {

                metaV = new Meta(0, -1, db.versionSequence().incr());
                metaV.size = metaV.size + 1;
                Key key_ = new Key(key_b.length, key_b, metaV.getVersion(), key);
                putDB(key_.convertBytes().toBytes(), value);
                putDB(key_b, metaV.convertMetaBytes().toBytes());
            }
            commit();
        } finally {
            release();
        }
    }


    public synchronized void putMayTTL(int ttl, Entry... entries) throws Exception {
        start();
        try {
            byte[] k_v = db.rocksDB().get(this.key_b);
            Meta metaV = addCheck(k_v);
            if (metaV != null) {
                setEntry(metaV, entries);
                putDB(this.key_b, metaV.convertMetaBytes().toBytes());
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new Meta(0, ttl, db.versionSequence().incr());
                setEntry(metaV, entries);
                putDB(key_b, metaV.convertMetaBytes().toBytes());
            }

            if (metaV.getTimestamp() != -1) {
                db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }
            commit();
        } finally {
            release();
        }
    }


    public Map<byte[], byte[]> get(byte[]... keys) throws Exception {
        Meta metaV = getMeta();
        Map<byte[], byte[]> map = new HashMap<>(keys.length);
        List<byte[]> keyList = new ArrayList<>();
        for (byte[] key : keys) {
            Key vkey = new Key(key_b.length, key_b, metaV.getVersion(), key);
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


    protected Meta getMeta() throws Exception {
        byte[] k_v = this.db.rocksDB().get(key_b);
        if (k_v == null) {
            throw new Exception("List do not exist");
        }
        Meta metaV = MetaD.build(k_v).convertMeta();
        long nowTime = System.currentTimeMillis();
        if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
            throw new Exception("List do not exist");
        }
        return metaV;
    }


    public synchronized void remove(byte[]... keys) throws Exception {
        try {
            start();
            Meta metaV = getMeta();
            for (byte[] key : keys) {
                Key vkey = new Key(key_b.length, key_b, metaV.getVersion(), key);
                deleteDB(vkey.convertBytes().toBytes());
                metaV.size = metaV.size + 1;
            }
            putDB(this.key_b, metaV.convertMetaBytes().toBytes());
            commit();
        } finally {
            release();
        }
    }

    public static synchronized void delete(byte[] key_b, byte[] k_v, DB db) throws Exception {
        RList rBase = new RList(db, "DEL");
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
    public void delete() throws Exception {
        try {
            start();
            Meta meta = getMeta();
            delete(key_b, this, meta);
        } finally {
            release();
        }
    }


    @Override
    public synchronized void deleteFast() throws Exception {
        Meta metaV = getMeta();
        deleteFast(this.key_b, this, metaV);
    }

    public static synchronized void deleteFast(byte[] key_b, DB db) throws Exception {
        RMap rBase = new RMap(db, "DEL");
        byte[] k_v = db.rocksDB().get(key_b);
        if (k_v == null) {
            return;
        }
        Meta metaV = MetaD.build(k_v).convertMeta();
        deleteFast(key_b, rBase, metaV);
    }


    @Override
    public int getTtl() throws Exception {
        Meta meta = getMeta();
        if (meta.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - meta.getTimestamp());
    }

    @Override
    public void delTtl() throws Exception {
        try {
            Meta metaV = getMeta();
            metaV.setTimestamp(-1);
            start();
            putDB(key_b, metaV.convertMetaBytes().toBytes());
            db.ttlZset().remove(metaV.convertMetaBytes().toBytes());
            commit();
        } finally {
            release();
        }
    }

    @Override
    public void ttl(int ttl) throws Exception {
        try {
            Meta metaV = getMeta();
            start();
            metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
            putDB(key_b, metaV.convertMetaBytes().toBytes());
            db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            commit();
        } finally {
            release();
        }
    }

    @Override
    public boolean isExist() throws RocksDBException {
        byte[] k_v = db.rocksDB().get(this.key_b);
        Meta meta = addCheck(k_v);
        return meta != null;
    }

    @Override
    public int size() throws Exception {
        Meta metaV = getMeta();
        return metaV.getSize();
    }

    @Override
    public Entry getEntry(RocksIterator iterator) {
        return null;
    }

    @Data
    @AllArgsConstructor
    public static class Entry extends RCollection.Entry {
        public byte[] key;
        public byte[] value;
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
