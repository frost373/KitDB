/*
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
    public final static byte[] HEAD_B = HEAD.getBytes();
    public final static byte[] HEAD_KEY_B = "m".getBytes();

    */
/*public RMap(DB db, String key) {
        this.key_b = (HEAD + key).getBytes(charset);
        this.db = db;
    }


    public static synchronized void delete(byte[] key_b, byte[] k_v, DB db) throws Exception {
        db.start();
        try {
            MetaD metaD = MetaD.build(k_v);
            delete(key_b, db, metaD);
            db.commit();
        } finally {
            db.release();
        }
    }

    private static void delete(byte[] key_b, DB db, MetaD metaD) {
        Meta metaV = metaD.convertMetaV();
        db.delete(key_b);
        Key vkey = new Key(key_b.length, key_b, metaV.getVersion(), null);
        deleteHead(vkey.getHead(), db);
        db.delete(ArrayKits.addAll("D".getBytes(charset), key_b, metaD.getVersion()));
    }

    public synchronized void putMayTTL(int ttl, Entry... entries) throws Exception {
        db.start();
        try {
            byte[] k_v = db.rocksDB().get(this.key_b);
            Meta metaV = addCheck(k_v);
            if (metaV != null) {
                setEntry(metaV, entries);
                db.put(this.key_b, metaV.convertMetaBytes().toBytes());
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new Meta(0, ttl, db.versionSequence().incr());
                setEntry(metaV, entries);
                db.put(key_b, metaV.convertMetaBytes().toBytes());
            }

            if (metaV.getTimestamp() != -1) {
                db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }
            db.commit();
        } finally {
            db.release();
        }
    }


    public synchronized void remove(byte[]... keys) throws Exception {
        try {
            db.start();
            Meta metaV = getMeta();
            for (byte[] key : keys) {
                Key vkey = new Key(key_b.length, key_b, metaV.getVersion(), key);
                db.delete(vkey.convertBytes().toBytes());
                metaV.size = metaV.size + 1;
            }
            db.put(this.key_b, metaV.convertMetaBytes().toBytes());
            db.commit();
        } finally {
            db.release();
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


    private void setEntry(Meta metaV, Entry[] entrys) {
        for (Entry entry : entrys) {
            metaV.size = metaV.size + 1;
            Key key = new Key(key_b.length, key_b, metaV.getVersion(), entry.key);
            db.put(key.convertBytes().toBytes(), entry.value);
        }
    }


    private Meta addCheck(byte[] k_v) {
        Meta metaV = null;
        if (k_v != null) {
            MetaD metaVD = MetaD.build(k_v);
            metaV = metaVD.convertMetaV();
            long nowTime = System.currentTimeMillis() / 1000;
            if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
                metaV = null;
                db.put(ArrayKits.addAll("D".getBytes(), key_b, metaVD.getVersion()), metaVD.toBytes());
            }
        }
        return metaV;
    }


    private Meta getMeta() throws Exception {
        byte[] k_v = this.db.rocksDB().get(key_b);
        if (k_v == null) {
            throw new Exception("List do not exist");
        }
        Meta metaV = MetaD.build(k_v).convertMetaV();
        long nowTime = System.currentTimeMillis();
        if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
            throw new Exception("List do not exist");
        }
        return metaV;
    }

    @Override
    public void delete() throws Exception {
        try {
            db.start();
            Meta metaV = getMeta();
            delete(key_b, db, metaV.convertMetaBytes());
        } finally {
            db.release();
        }

    }

    @Override
    public void deleteFast() throws Exception {
        db.start();
        try {
            Meta metaV = getMeta();
            MetaD metaVD = metaV.convertMetaBytes();
            db.put(ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion()), metaVD.toBytes());
            db.delete(key_b);
            db.commit();
        } finally {
            db.release();
        }
    }

    @Override
    public int getTtl() throws Exception {
        Meta metaV = getMeta();
        if (metaV.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - metaV.getTimestamp());
    }

    @Override
    public void delTtl() throws Exception {
        db.start();
        try {
            Meta metaV = getMeta();
            metaV.setTimestamp(-1);
            db.put(key_b, metaV.convertMetaBytes().toBytes());
            db.ttlZset().remove(metaV.convertMetaBytes().toBytes());
            db.commit();
        } finally {
            db.release();
        }
    }

    @Override
    public void ttl(int ttl) throws Exception {
        try {
            Meta metaV = getMeta();
            db.start();
            metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
            db.put(key_b, metaV.convertMetaBytes().toBytes());
            db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            db.commit();
        } finally {
            db.release();
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
    public RCollection.Entry getEntry(RocksIterator iterator) {
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
    public static class Meta{
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
    public static class MetaD {
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

        public Meta convertMetaV() {
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
    }*//*


}
*/
