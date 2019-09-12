package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.MetaAbs;
import top.thinkin.lightd.base.MetaDAbs;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.data.ReservedWords;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * A List
 */
public class RList extends RCollection {

    public final static String HEAD = KeyEnum.LIST.getKey();
    public final static byte[] HEAD_VALUE_B = KeyEnum.LIST_VALUE.getBytes();
    public final static byte[] HEAD_B = HEAD.getBytes();


    protected RList(DB db) {
        super(false, 128);
        this.db = db;
    }

    public void add(String key, byte[] v) throws Exception {
        addMayTTL(key, v, -1);
    }


    public void addAll(String key, List<byte[]> vs) throws Exception {
        addAllMayTTL(key, vs, -1);
    }


    protected byte[] getKey(String key) {
        DAssert.notNull(key, ErrorType.NULL, "Key is null");
        return ArrayKits.addAll(HEAD_B, key.getBytes(charset));
    }

    public int size(String key) throws Exception {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return 0;
        }
        return metaV.getSize();
    }

    private MetaV addCheck(byte[] k_v) {
        MetaV metaV = null;
        if (k_v != null) {
            MetaVD metaVD = MetaVD.build(k_v);
            metaV = metaVD.convertMeta();
            if (metaV.getTimestamp() != -1 && (System.currentTimeMillis() / 1000) - metaV.getTimestamp() >= 0) {
                metaV = null;
            }
        }
        return metaV;
    }


    protected MetaV getMeta(byte[] key_b) throws Exception {

        byte[] k_v = getDB(key_b, SstColumnFamily.META);
        if (k_v == null) return null;

        MetaV metaV = MetaVD.build(k_v).convertMeta();
        if (metaV.getTimestamp() != -1 && (System.currentTimeMillis() / 1000) - metaV.getTimestamp() >= 0) {
            metaV = null;
        }

        return metaV;
    }

    public boolean isExist(String key) throws RocksDBException {
        byte[] key_b = getKey(key);
        byte[] k_v = getDB(key_b, SstColumnFamily.META);
        MetaV metaV = addCheck(k_v);

        return metaV != null;
    }

    public void ttl(String key, int ttl) throws Exception {
        lock.lock(key);
        byte[] key_b = getKey(key);
        try {
            start();
            MetaV metaV = getMeta(key_b);

            DAssert.notNull(metaV, ErrorType.NOT_EXIST, "The List does not exist.");

            if (ttl != -1) {
                metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
            }
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            commit();
            db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
        } finally {
            lock.unlock(key);
            release();
        }
    }

    public void delTtl(String key) throws Exception {
        lock.lock(key);
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return;
        }
        try {
            start();
            metaV.setTimestamp(-1);
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            commit();
            db.getzSet().remove(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes());
        } finally {
            lock.unlock(key);
            release();
        }
    }

    public int getTtl(String key) throws Exception {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return 0;
        }
        if (metaV.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - metaV.getTimestamp());
    }


    private void delete(byte[] key_b, MetaV metaV) throws Exception {
        ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.left);
        MetaVD metaVD = metaV.convertMetaBytes();
        ValueKD valueKD = valueK_seek.convertValueBytes();
        byte[] heads = valueKD.toHeadBytes();
        deleteHead(heads, SstColumnFamily.DEFAULT);
        byte[] key = ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion());
        deleteDB(key, SstColumnFamily.DEFAULT);
    }


    protected synchronized void deleteByClear(byte[] key_b, MetaV metaV) throws Exception {
        try {
            start();
            delete(key_b, metaV);
        } finally {
            release();
        }
    }


    public void delete(String key) throws Exception {
        lock.lock(key);
        byte[] key_b = getKey(key);

        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return;
        }
        try {
            start();
            deleteDB(key_b, SstColumnFamily.META);
            delete(key_b, metaV);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    @Override
    public KeyIterator getKeyIterator() throws Exception {
        return getKeyIterator(HEAD_B);
    }


    public List<byte[]> range(String key, long start, long end) throws Exception {
        byte[] key_b = getKey(key);

        List<byte[]> list = new ArrayList<>();

        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return list;
        }
        ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), start);
        try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
            ValueKD valueKD = valueK_seek.convertValueBytes();
            byte[] heads = valueKD.toHeadBytes();
            iterator.seek(valueKD.toBytes());
            long index = 0;
            while (iterator.isValid() && index < end) {
                byte[] key_bs = iterator.key();
                if (!BytesUtil.checkHead(heads, key_bs)) break;
                ValueK valueK = ValueKD.build(key_bs).convertValue();
                index = valueK.getIndex();
                list.add(iterator.value());
                iterator.next();
            }
        } catch (Exception e) {
            throw e;
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    public RIterator<RList> iterator(String key) throws Exception {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return null;
        }
        ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.left);
        RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT);
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

    public List<byte[]> blpop(String key, int num) throws Exception {
        lock.lock(key);
        byte[] key_b = getKey(key);

        List<byte[]> list = new ArrayList<>();
        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return list;
        }
        try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
            final int maxCount = num > 0 ? num : Integer.MAX_VALUE;
            ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.left);
            List<byte[]> delete_keys = new ArrayList<>();
            ValueKD valueKD = valueK_seek.convertValueBytes();
            byte[] heads = valueKD.toHeadBytes();
            iterator.seek(valueKD.toBytes());
            int count = 0;
            while (iterator.isValid() && count++ < maxCount) {
                byte[] ikey = iterator.key();
                if (!BytesUtil.checkHead(heads, ikey)) break;
                ValueKD key_bytes = ValueKD.build(ikey);
                delete_keys.add(ikey);
                list.add(iterator.value());
                metaV.setLeft(key_bytes.getIndexV());
                iterator.next();
            }

            metaV.setSize(metaV.getSize() - delete_keys.size());
            if (metaV.getSize() != 0) {
                byte[] ikey = iterator.key();
                ValueKD key_bytes = ValueKD.build(ikey);
                metaV.setLeft(key_bytes.getIndexV());
            }
            start();
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            for (byte[] delete_key : delete_keys) {
                deleteDB(delete_key, SstColumnFamily.DEFAULT);
            }
            commit();
            return list;
        } catch (Exception e) {
            throw e;
        } finally {
            lock.unlock(key);
            release();
        }
    }



    public void addAllMayTTL(String key, List<byte[]> vs, int ttl) throws Exception {
        lock.lock(key);
        byte[] key_b = getKey(key);
        try {
            start();
            byte[] k_v = getDB(key_b, SstColumnFamily.META);
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
                    putDB(valueKD.toBytes(), v, SstColumnFamily.DEFAULT);
                }
                //写入Meta
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
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
                    putDB(valueK.convertValueBytes().toBytes(), v, SstColumnFamily.DEFAULT);
                }
                //写入Meta
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            }
            commit();
            if (ttl != -1) {
                db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }
        } finally {
            lock.unlock(key);
            release();
        }
    }


    public void deleteFast(String key) throws Exception {
        lock.lock(key);
        byte[] key_b = getKey(key);

        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return;
        }
        try {
            deleteFast(key_b, metaV);
        } finally {
            lock.unlock(key);
        }
    }



    /**
     * 如果新建则设置设置TTL。如果已存在则不设置
     *
     * @param v
     * @param ttl
     * @throws RocksDBException
     */
    public void addMayTTL(String key, byte[] v, int ttl) throws Exception {
        lock.lock(key);
        byte[] key_b = getKey(key);
        try {
            start();
            byte[] k_v = getDB(key_b, SstColumnFamily.META);
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
                putDB(valueKD.toBytes(), v, SstColumnFamily.DEFAULT);
                //写入Meta
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new MetaV(1, 0, 0, ttl, db.versionSequence().incr());
                ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.right);
                //写入Value
                putDB(valueK.convertValueBytes().toBytes(), v, SstColumnFamily.DEFAULT);

                //写入Meta
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);

            }
            commit();
            if (ttl != -1) {
                db.getzSet().add(ReservedWords.ZSET_KEYS.TTL, metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }
        } finally {
            lock.unlock(key);
            release();
        }
    }



    public byte[] get(String key, long i) throws Exception {
        byte[] key_b = getKey(key);

        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return null;
        }
        ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), i);
        return getDB(valueK.convertValueBytes().toBytes(), SstColumnFamily.DEFAULT);
    }


    public List<byte[]> get(String key, List<Long> is) throws Exception {
        byte[] key_b = getKey(key);

        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return new ArrayList<>();
        }
        List<byte[]> list = new ArrayList<>(is.size());

        for (long i : is) {
            ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), i);
            list.add(getDB(valueK.convertValueBytes().toBytes(), SstColumnFamily.DEFAULT));
        }
        return list;
    }


    public Long left(String key) throws Exception {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return null;
        }
        return metaV.left;
    }

    public Long right(String key) throws Exception {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        if (metaV == null) {
            return null;
        }
        return metaV.right;
    }

    public void set(String key, long i, byte[] v) throws Exception {
        lock.lock(key);
        try {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return;
            }
            DAssert.isTrue(i >= metaV.left && i <= metaV.right, ErrorType.EMPTY, "index not exist");
            start();

            ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), i);
            ValueKD valueKD = valueK.convertValueBytes();
            //写入Value
            putDB(valueKD.toBytes(), v, SstColumnFamily.DEFAULT);
            commit();
        } finally {
            lock.unlock(key);
            release();
        }
    }

    @Data
    @AllArgsConstructor
    public class Entry extends REntry {
        private long index;
        private byte[] value;
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
            byte[] value = ArrayKits.addAll(HEAD_B, this.size, this.left, this.right, this.timestamp, this.version);
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
            byte[] key = ArrayKits.addAll(HEAD_VALUE_B, this.k_size, this.key, this.version, this.index);
            return key;
        }

        public long getIndexV() {
            return ArrayKits.bytesToLong(this.index);
        }

        public byte[] toHeadBytes() {
            byte[] key = ArrayKits.addAll(HEAD_VALUE_B, this.k_size, this.key, this.version);
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
