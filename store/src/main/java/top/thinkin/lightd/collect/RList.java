package top.thinkin.lightd.collect;

import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.exception.NonExistException;

import java.util.ArrayList;
import java.util.List;

/**
 * A List
 */
public class RList extends RBase implements RCollection {

    public final static String HEAD = "L";
    public final static String HEAD_VALUE = "l";
    public static byte[] HEAD_B = HEAD.getBytes();

    public RList(DB db, String key) {
        this.key_b = ArrayKits.addAll(HEAD_B, key.getBytes(charset));
        this.db = db;
    }

    public int size() throws Exception {
        MetaV metaV = getMetaV();
        return metaV.getSize();
    }

    @Override
    protected MetaV getMetaV() throws Exception {
        byte[] k_v = db.rocksDB().get(key_b);
        if (k_v == null) throw new NonExistException("List");

        MetaV metaV = MetaVD.build(k_v).convertMeta();
        long nowTime = System.currentTimeMillis();
        if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) throw new NonExistException("List");

        return metaV;
    }

    public boolean isExist() throws RocksDBException {
        byte[] k_v = db.rocksDB().get(this.key_b);
        MetaV metaV = addCheck(k_v);

        return metaV != null;
    }

    public MetaV getMeta() throws Exception {
        return getMetaV();
    }



    public synchronized void ttl(int ttl) throws Exception {
        start();
        try {
            MetaV metaV = getMetaV();
            metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
            put(key_b, metaV.convertMetaBytes().toBytes());
            db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            commit();
        } finally {
            release();
        }
    }

    public synchronized void delTtl() throws Exception {
        start();
        try {
            MetaV metaV = getMetaV();
            metaV.setTimestamp(-1);
            put(key_b, metaV.convertMetaBytes().toBytes());
            db.ttlZset().remove(metaV.convertMetaBytes().toBytes());
            commit();
        } finally {
            release();
        }
    }

    public int getTtl() throws Exception {
        MetaV metaV = getMetaV();
        if (metaV.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - metaV.getTimestamp());
    }


    public static synchronized void deleteFast(byte[] key_b, DB db) throws Exception {
        RList rBase = new RList(db, "DEL");
        byte[] k_v = db.rocksDB().get(key_b);
        if (k_v == null) {
            return;
        }
        MetaV metaV = MetaVD.build(k_v).convertMeta();
        deleteFast(key_b, rBase, metaV);
    }

    public static synchronized void delete(byte[] key_b, byte[] k_v, DB db) throws Exception {
        RList rBase = new RList(db, "DEL");
        rBase.start();
        try {
            MetaV metaV = MetaVD.build(k_v).convertMeta();
            delete(key_b, rBase, metaV);
            rBase.commit();
        } finally {
            rBase.release();
        }
    }

    private static void delete(byte[] key_b, RBase rBase, MetaV metaV) throws Exception {
        ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.left);
        MetaVD metaVD = metaV.convertMetaBytes();
        ValueKD valueKD = valueK_seek.convertValueBytes();
        byte[] heads = valueKD.toHeadBytes();
        deleteHead(heads, rBase);
        byte[] key = ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion());
        rBase.delete(key);
    }

    public synchronized void delete() throws Exception {
        start();
        try {
            MetaV metaV = getMetaV();
            delete(key_b);
            delete(key_b, this, metaV);
            commit();
        } finally {
            release();
        }
    }

    public List<byte[]> range(long start, long end) throws Exception {
        MetaV metaV = getMetaV();
        List<byte[]> list = new ArrayList<>();
        ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), start);
        try (final RocksIterator iterator = db.rocksDB().newIterator()) {
            ValueKD valueKD = valueK_seek.convertValueBytes();
            byte[] heads = valueKD.toHeadBytes();
            iterator.seek(valueKD.toBytes());
            long index = 0;
            while (iterator.isValid() && index < end) {
                byte[] key_bs = iterator.key();
                if (!BytesUtil.checkHead(heads, key_bs)) break;
                ValueK key = ValueKD.build(key_bs).convertValue();
                index = key.getIndex();
                list.add(iterator.value());
                iterator.next();
            }
        } catch (Exception e) {
            throw e;
        }
        return list;
    }

    public RIterator<RList> iterator() throws Exception {
        MetaV metaV = getMetaV();
        ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.left);
        RocksIterator iterator = db.rocksDB().newIterator();
        ValueKD valueKD = valueK_seek.convertValueBytes();
        iterator.seek(valueKD.toBytes());
        RIterator<RList> rIterator = new RIterator<>(iterator, this, valueKD.toHeadBytes());
        return rIterator;
    }

    public RList.Entry getEntry(RocksIterator iterator) {
        byte[] key_bs = iterator.key();
        if (key_bs == null) {
            return null;
        }
        ValueK key = ValueKD.build(key_bs).convertValue();
        RList.Entry entry = new RList.Entry(key.index, iterator.value());
        return entry;
    }

    public synchronized List<byte[]> blpop(int num) throws Exception {
        start();
        try (final RocksIterator iterator = db.rocksDB().newIterator()) {
            MetaV metaV = getMetaV();
            List<byte[]> list = new ArrayList<>();
            final int maxCount = num > 0 ? num : Integer.MAX_VALUE;
            ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.left);
            List<byte[]> delete_keys = new ArrayList<>();
            ValueKD valueKD = valueK_seek.convertValueBytes();
            byte[] heads = valueKD.toHeadBytes();
            iterator.seek(valueKD.toBytes());
            int count = 0;
            while (iterator.isValid() && count++ < maxCount) {
                byte[] key = iterator.key();
                if (!BytesUtil.checkHead(heads, key)) break;
                ValueKD key_bytes = ValueKD.build(key);
                delete_keys.add(key);
                list.add(iterator.value());
                metaV.setLeft(key_bytes.getIndexV());
                iterator.next();
            }

            metaV.setSize(metaV.getSize() - delete_keys.size());
            if (metaV.getSize() != 0) {
                byte[] key = iterator.key();
                ValueKD key_bytes = ValueKD.build(key);
                metaV.setLeft(key_bytes.getIndexV());
            }
            put(key_b, metaV.convertMetaBytes().toBytes());
            for (byte[] delete_key : delete_keys) {
                delete(delete_key);
            }
            commit();
            return list;
        } catch (Exception e) {
            throw e;
        } finally {
            release();
        }
    }

    public synchronized void addAll(List<byte[]> vs) throws Exception {
        addAllMayTTL(vs, -1);
    }

    public synchronized void addAllMayTTL(List<byte[]> vs, int ttl) throws Exception {
        start();
        try {
            byte[] k_v = db.rocksDB().get(this.key_b);
            MetaV metaV = addCheck(k_v);

            if (metaV != null) {
                //写入Value
                for (byte[] v : vs) {
                    metaV.size = metaV.size + 1;
                    metaV.right = metaV.right + 1;
                    if (metaV.size == 1) {
                        metaV.left = metaV.right;
                    }
                    ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.right);
                    ValueKD valueKD = valueK.convertValueBytes();
                    put(valueKD.toBytes(), v);
                }
                //写入Meta
                put(key_b, metaV.convertMetaBytes().toBytes());
            } else {

                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new MetaV(0, 0, -1, ttl, db.versionSequence().incr());
                //写入Value
                for (byte[] v : vs) {
                    metaV.size = metaV.size + 1;
                    metaV.right = metaV.right + 1;
                    if (metaV.size == 1) {
                        metaV.left = metaV.right;
                    }
                    ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.right);
                    put(valueK.convertValueBytes().toBytes(), v);
                }
                //写入Meta
                put(key_b, metaV.convertMetaBytes().toBytes());
            }
            if (ttl != -1) {
                db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }

            commit();
        } finally {
            release();
        }
    }

    private MetaV addCheck(byte[] k_v) throws RocksDBException {
        MetaV metaV = null;
        if (k_v != null) {
            MetaVD metaVD = MetaVD.build(k_v);
            metaV = metaVD.convertMeta();
            long nowTime = System.currentTimeMillis() / 1000;
            if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
                metaV = null;
                put(ArrayKits.addAll("D".getBytes(), key_b, metaVD.getVersion()), metaVD.toBytes());
            }
        }
        return metaV;
    }

    /**
     * 如果新建则设置设置TTL。如果已存在则不设置
     *
     * @param v
     * @param ttl
     * @throws RocksDBException
     */
    public synchronized void addMayTTL(byte[] v, int ttl) throws Exception {
        start();
        try {
            byte[] k_v = db.rocksDB().get(this.key_b);
            MetaV metaV = addCheck(k_v);
            if (metaV != null) {
                metaV.size = metaV.size + 1;
                metaV.right = metaV.right + 1;
                if (metaV.size == 1) {
                    metaV.left = metaV.right;
                }
                ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.right);
                ValueKD valueKD = valueK.convertValueBytes();
                //写入Value
                put(valueKD.toBytes(), v);
                //写入Meta
                put(key_b, metaV.convertMetaBytes().toBytes());
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new MetaV(1, 0, 0, ttl, db.versionSequence().incr());
                ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.right);
                //写入Value
                put(valueK.convertValueBytes().toBytes(), v);

                //写入Meta
                put(key_b, metaV.convertMetaBytes().toBytes());

            }

            if (ttl != -1) {
                db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }

            commit();
        } finally {
            release();
        }
    }

    public synchronized void add(byte[] v) throws Exception {
        addMayTTL(v, -1);
    }

    public byte[] get(long i) throws Exception {
        MetaV metaV = getMetaV();
        ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), i);
        return db.rocksDB().get(valueK.convertValueBytes().toBytes());
    }


    public List<byte[]> get(long... is) throws Exception {
        MetaV metaV = getMetaV();
        List<byte[]> list = new ArrayList<>(is.length);
        for (long i : is) {
            ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), i);
            list.add(db.rocksDB().get(valueK.convertValueBytes().toBytes()));
        }
        return list;
    }


    public synchronized void remove(long i) throws Exception {
        start();
        try {
            MetaV metaV = getMetaV();
            ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), i);
            delete(valueK.convertValueBytes().toBytes());
            commit();
        } finally {
            release();
        }
    }

    @Data
    @AllArgsConstructor
    public class Entry extends RCollection.Entry {
        public long index;
        public byte[] value;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MetaV extends MetaAbs {
        private int size;
        private long left;
        private long right;
        private int timestamp;
        private int version;

        public MetaVD convertMetaBytes() {
            MetaVD metaVD = new MetaVD();
            metaVD.setSize(ArrayKits.intToBytes(this.size));
            metaVD.setLeft(ArrayKits.longToBytes(this.left));
            metaVD.setRight(ArrayKits.longToBytes(this.right));
            metaVD.setTimestamp(ArrayKits.intToBytes(this.timestamp));
            metaVD.setVersion(ArrayKits.intToBytes(this.version));
            return metaVD;
        }
    }

    @Data
    public static class MetaVD extends MetaDAbs {
        public static MetaVD build(byte[] bytes) {
            MetaVD metaVD = new MetaVD();
            metaVD.setSize(ArrayUtil.sub(bytes, 1, 5));
            metaVD.setLeft(ArrayUtil.sub(bytes, 5, 13));
            metaVD.setRight(ArrayUtil.sub(bytes, 13, 21));
            metaVD.setTimestamp(ArrayUtil.sub(bytes, 21, 25));
            metaVD.setVersion(ArrayUtil.sub(bytes, 25, 29));
            return metaVD;
        }

        public MetaV convertMeta() {
            MetaV metaV = new MetaV();
            metaV.setSize(ArrayKits.bytesToInt(this.size, 0));
            metaV.setLeft(ArrayKits.bytesToLong(this.left));
            metaV.setRight(ArrayKits.bytesToLong(this.right));
            metaV.setTimestamp(ArrayKits.bytesToInt(this.timestamp, 0));
            metaV.setVersion(ArrayKits.bytesToInt(this.version, 0));
            return metaV;
        }

        public int getVersionValue() {
            return ArrayKits.bytesToInt(this.version, 0);
        }

        public byte[] toBytes() {
            byte[] value = ArrayKits.addAll(HEAD.getBytes(charset), this.size, this.left, this.right, this.timestamp, this.version);
            return value;
        }

        private byte[] size;
        private byte[] left;
        private byte[] right;
        private byte[] timestamp;
        private byte[] version;


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ValueK {
        private int k_size;
        private byte[] key;
        private int version;
        private long index;

        public ValueKD convertValueBytes() {
            ValueKD valueKD = new ValueKD();
            valueKD.setK_size(ArrayKits.intToBytes(this.getK_size()));
            valueKD.setKey(this.getKey());
            valueKD.setVersion(ArrayKits.intToBytes(this.getVersion()));
            valueKD.setIndex(ArrayKits.longToBytes(this.getIndex()));
            return valueKD;
        }

    }

    @Data
    public static class ValueKD {
        public byte[] toBytes() {
            byte[] key = ArrayKits.addAll(HEAD_VALUE.getBytes(charset), this.k_size, this.key, this.version, this.index);
            return key;
        }

        public long getIndexV() {
            return ArrayKits.bytesToLong(this.index);
        }

        public byte[] toHeadBytes() {
            byte[] key = ArrayKits.addAll(HEAD_VALUE.getBytes(charset), this.k_size, this.key, this.version);
            return key;
        }

        public ValueK convertValue() {
            ValueK valueK = new ValueK();
            valueK.setIndex(ArrayKits.bytesToLong(this.index));
            valueK.setK_size(ArrayKits.bytesToInt(this.k_size, 0));
            valueK.setKey(this.key);
            valueK.setVersion(ArrayKits.bytesToInt(this.version, 0));
            return valueK;
        }


        public static ValueKD build(byte[] bytes) {
            ValueKD valueKD = new ValueKD();
            valueKD.setK_size(ArrayUtil.sub(bytes, 1, 5));
            int position = ArrayKits.bytesToInt(valueKD.getK_size(), 0);
            valueKD.setKey(ArrayUtil.sub(bytes, 5, position = 5 + position));
            valueKD.setVersion(ArrayUtil.sub(bytes, position, position = position + 4));
            valueKD.setIndex(ArrayUtil.sub(bytes, position, position + 8));
            return valueKD;
        }

        private byte[] k_size;
        private byte[] key;
        private byte[] version;
        private byte[] index;
    }


}
