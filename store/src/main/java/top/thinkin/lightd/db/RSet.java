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
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.List;

public class RSet extends RCollection {

    public static String HEAD = KeyEnum.SET.getKey();
    public static byte[] HEAD_B = HEAD.getBytes();
    public static byte[] HEAD_V_B = KeyEnum.SET_V.getBytes();
    final RocksIterator iterator;

    public RSet(DB db, String key) {
        this.db = db;
        this.key_b = ArrayKits.addAll(HEAD_B, key.getBytes(charset));
        this.iterator = db.rocksDB().newIterator();
    }


    /**
     * 移除并返回集合中的n个元素
     *
     * @param num
     * @return
     * @throws Exception
     */
    public synchronized List<byte[]> pop(int num) throws Exception {
        start();
        List<byte[]> values = new ArrayList<>();
        try (final RocksIterator iterator = db.rocksDB().newIterator()) {
            List<byte[]> dels = new ArrayList<>();
            MetaV metaV = getMeta();
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), ArrayKits.intToBytes(0));
            byte[] head = sData.getHead();
            iterator.seek(head);
            int count = 0;
            while (iterator.isValid() && count++ < num) {
                byte[] key_bs = iterator.key();
                //iterator.value();
                if (!BytesUtil.checkHead(head, key_bs)) return values;
                SDataD sDataD = SDataD.build(key_bs);
                values.add(sDataD.getValue());
                dels.add(key_bs);
                iterator.next();
            }
            try {
                putDB(key_b, metaV.convertMetaBytes().toBytes());
                removeDo(metaV, dels);
                commit();
            } finally {
                release();
            }
            return values;

        }


    }

    /**
     * 移除集合中一个或多个成员
     *
     * @param values
     * @throws Exception
     */
    public synchronized void remove(byte[]... values) throws Exception {
        MetaV metaV = getMeta();
        List<byte[]> dels = new ArrayList<>();
        for (byte[] v : values) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
            SDataD sDataD = sData.convertBytes();
            byte[] scoreD = db.rocksDB().get(sDataD.toBytes());
            if (scoreD != null) {
                dels.add(sDataD.toBytes());
            }
        }
        start();
        try {
            removeDo(metaV, dels);
            putDB(key_b, metaV.convertMetaBytes().toBytes());
            commit();
        } catch (Exception e) {
            release();
        }
    }

    public boolean isMember(byte[] value) throws RocksDBException {
        byte[] k_v = db.rocksDB().get(this.key_b);
        MetaV metaV = addCheck(k_v);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), value);
        return db.rocksDB().get(sData.convertBytes().toBytes()) != null;
    }

    /**
     * 向集合添加一个或多个成员,如果集合在添加时不存在，则为集合设置TTL
     *
     * @param ttl
     * @param values
     * @throws Exception
     */
    public synchronized void addMayTTL(int ttl, byte[]... values) throws Exception {

        DAssert.isTrue(ArrayKits.noRepeate(values), ErrorType.REPEATED_KEY, "Repeated memebers");
        start();
        try {
            byte[] k_v = db.rocksDB().get(this.key_b);
            MetaV metaV = addCheck(k_v);
            if (metaV != null) {
                setEntry(metaV, values);
                putDB(this.key_b, metaV.convertMetaBytes().toBytes());
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new MetaV(0, ttl, db.versionSequence().incr());
                setEntry(metaV, values);
                putDB(this.key_b, metaV.convertMetaBytes().toBytes());
            }
            if (metaV.getTimestamp() != -1) {
                db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
            }
            commit();
        } finally {
            release();
        }
    }

    /**
     * 向集合添加一个或多个成员
     *
     * @param values
     * @throws Exception
     */
    public synchronized void add(byte[]... values) throws Exception {
        addMayTTL(-1, values);
    }


    @Override
    public synchronized void delete() throws Exception {
        try {
            start();
            MetaV metaV = getMeta();
            delete(this.key_b, this, metaV.convertMetaBytes());
            commit();
        } finally {
            release();
        }
    }

    @Override
    public synchronized void deleteFast() throws Exception {
        RSet rBase = new RSet(db, "DEL");
        byte[] k_v = db.rocksDB().get(key_b);
        if (k_v == null) {
            return;
        }
        MetaV meta = MetaD.build(k_v).convertMetaV();
        deleteFast(key_b, rBase, meta);
    }

    @Override
    public RIterator<RSet> iterator() throws Exception {
        MetaV metaV = getMeta();
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), "".getBytes());
        RocksIterator iterator = db.rocksDB().newIterator();
        iterator.seek(sData.getHead());
        return new RIterator<>(iterator, this, sData.getHead());
    }


    @Override
    public int getTtl() throws Exception {
        MetaV metaV = getMeta();
        if (metaV.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - metaV.getTimestamp());
    }

    @Override
    public synchronized void delTtl() throws Exception {
        try {
            MetaV metaV = getMeta();
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
    public synchronized void ttl(int ttl) throws Exception {
        try {
            MetaV metaV = getMeta();
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
        MetaV meta = addCheck(k_v);
        return meta != null;
    }

    @Override
    public int size() throws Exception {
        MetaV metaV = getMeta();
        return metaV.getSize();
    }

    @Override
    public Entry getEntry(RocksIterator iterator) {
        byte[] key_bs = iterator.key();
        if (key_bs == null) {
            return null;
        }
        SData sData = SDataD.build(key_bs).convertValue();
        Entry entry = new Entry(sData.value);
        return entry;
    }

    private MetaV addCheck(byte[] k_v) throws RocksDBException {
        MetaV metaV = null;
        if (k_v != null) {
            MetaD metaD = MetaD.build(k_v);
            metaV = metaD.convertMetaV();
            long nowTime = System.currentTimeMillis() / 1000;
            if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
                metaV = null;
                db.rocksDB().put(ArrayKits.addAll("D".getBytes(), key_b, metaD.getVersion()), metaD.toBytes());
            }
        }
        return metaV;
    }

    private void removeDo(MetaV metaV, List<byte[]> dels) {
        for (byte[] del : dels) {
            metaV.setSize(metaV.getSize() - 1);
            deleteDB(del);
        }
    }


    private void setEntry(MetaV metaV, byte[][] values) throws RocksDBException {
        for (byte[] value : values) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), value);
            byte[] member = sData.convertBytes().toBytes();
            if (db.rocksDB().get(member) == null) {
                metaV.size = metaV.size + 1;
            }
            putDB(sData.convertBytes().toBytes(), "".getBytes());
        }
    }


    private static void delete(byte[] key_b, RBase rBase, MetaD metaD) {
        MetaV metaV = metaD.convertMetaV();
        rBase.deleteDB(key_b);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), null);
        deleteHead(sData.getHead(), rBase);
        rBase.deleteDB(ArrayKits.addAll("D".getBytes(charset), key_b, metaD.getVersion()));
    }

    @Override
    protected MetaV getMeta() throws Exception {
        byte[] k_v = this.db.rocksDB().get(key_b);
        if (k_v == null) {
            throw new Exception("List do not exist");
        }
        MetaV metaV = MetaD.build(k_v).convertMetaV();
        long nowTime = System.currentTimeMillis();
        if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
            throw new Exception("List do not exist");
        }
        return metaV;
    }

    @Data
    @AllArgsConstructor
    public static class Entry extends RCollection.Entry {
        private byte[] value;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MetaV extends MetaAbs {
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

        public MetaV convertMetaV() {
            MetaV metaV = new MetaV();
            metaV.setSize(ArrayKits.bytesToInt(this.size, 0));
            metaV.setTimestamp(ArrayKits.bytesToInt(this.timestamp, 0));
            metaV.setVersion(ArrayKits.bytesToInt(this.version, 0));
            return metaV;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SData {
        private int mapKeySize;
        private byte[] mapKey;
        private int version;
        private byte[] value;


        public SDataD convertBytes() {
            SDataD sDataD = new SDataD();
            sDataD.setMapKeySize(ArrayKits.intToBytes(this.mapKeySize));
            sDataD.setMapKey(this.mapKey);
            sDataD.setVersion(ArrayKits.intToBytes(this.version));
            sDataD.setValue(this.value);
            return sDataD;
        }


        public byte[] getHead() {
            byte[] value = ArrayKits.addAll(HEAD_V_B, ArrayKits.intToBytes(this.mapKeySize),
                    this.mapKey, ArrayKits.intToBytes(this.version));
            return value;
        }

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SDataD {
        private byte[] mapKeySize;
        private byte[] mapKey;
        private byte[] version;
        private byte[] value;

        public byte[] toBytes() {
            byte[] value = ArrayKits.addAll(HEAD_V_B, this.mapKeySize, this.mapKey, this.version, this.value);
            return value;
        }

        public static SDataD build(byte[] bytes) {
            SDataD sDataD = new SDataD();
            sDataD.setMapKeySize(ArrayUtil.sub(bytes, 1, 5));
            int position = ArrayKits.bytesToInt(sDataD.getMapKeySize(), 0);
            sDataD.setMapKey(ArrayUtil.sub(bytes, 5, position = 5 + position));
            sDataD.setVersion(ArrayUtil.sub(bytes, position, position = position + 4));
            sDataD.setValue(ArrayUtil.sub(bytes, position, bytes.length));
            return sDataD;
        }

        public SData convertValue() {
            SData sData = new SData();
            sData.setMapKeySize(ArrayKits.bytesToInt(this.mapKeySize, 0));
            sData.setMapKey(this.mapKey);
            sData.setVersion(ArrayKits.bytesToInt(this.version, 0));
            sData.setValue(this.value);
            return sData;
        }
    }
}
