package top.thinkin.lightd.collect;

import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.java.Log;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

@Log
public class ZSet implements RCollection {
    private byte[] key_b;
    public static String HEAD = "Z";

    public static byte[] HEAD_B = HEAD.getBytes();

    public static byte[] HEAD_SCORE_B = "z".getBytes();

    public static byte[] HEAD_V_B = "a".getBytes();
    private DB db;

    private static Charset charset = Charset.forName("UTF-8");


    public synchronized void add(byte[] v, long score) throws Exception {
        addMayTTL(-1, new Entry(score, v));
    }

    public synchronized void add(int ttl, Entry... entrys) throws Exception {
        addMayTTL(ttl, entrys);
    }

    public synchronized void addMayTTL(int ttl, byte[] v, long score) throws Exception {
        addMayTTL(ttl, new Entry(score, v));
    }

    public synchronized void addMayTTL(int ttl, Entry... entrys) throws Exception {
        db.start();
        try {
            byte[] k_v = db.rocksDB().get(this.key_b);
            MetaV metaV = addCheck(k_v);
            if (metaV != null) {
                setEntry(metaV, entrys);
                db.put(this.key_b, metaV.convertMetaBytes().toBytes());
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new MetaV(0, ttl, db.versionSequence().getSequence());
                setEntry(metaV, entrys);
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


    private void setEntry(MetaV metaV, Entry[] entrys) throws Exception {
        for (Entry entry : entrys) {
            metaV.size = metaV.size + 1;
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), entry.value);
            ZData zData = new ZData(key_b.length, key_b, metaV.getVersion(), entry.score, entry.value);
            db.put(sData.convertBytes().toBytes(), ArrayKits.longToBytes(entry.score));
            db.put(zData.convertBytes().toBytes(), "test".getBytes());
        }
    }

    /**
     * 返回指定区间分数的成员
     *
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public List<Entry> range(long start, long end) throws Exception {
        List<Entry> entries = new ArrayList<>();
        MetaV metaV = getMetaV();
        ZData zData = new ZData(key_b.length, key_b, metaV.getVersion(), start, "".getBytes());

        byte[] seek = zData.getSeek();
        byte[] head = zData.getHead();
        final RocksIterator iterator = db.rocksDB().newIterator();
        iterator.seek(seek);
        long index = 0;
        while (iterator.isValid() && index <= end) {
            byte[] key_bs = iterator.key();
            if (!BytesUtil.checkHead(head, key_bs)) break;

            ZData key = ZDataD.build(key_bs).convertValue();
            index = key.getScore();
            if (index > end) {
                break;
            }
            entries.add(new Entry(index, key.value));
            iterator.next();
        }


        return entries;
    }

    public RIterator<ZSet> iterator() throws Exception {
        MetaV metaV = getMetaV();
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), "".getBytes());
        RocksIterator iterator = db.rocksDB().newIterator();
        iterator.seek(sData.getHead());
        RIterator<ZSet> rIterator = new RIterator<>(iterator, this, sData.getHead());
        return rIterator;
    }


    /**
     * 返回指定区间分数的成员并删除
     *
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public synchronized List<Entry> rangeDel(long start, long end) throws Exception {
        List<Entry> entries = new ArrayList<>();
        MetaV metaV = getMetaV();
        ZData zData = new ZData(key_b.length, key_b, metaV.getVersion(), start, "".getBytes());

        byte[] seek = zData.getSeek();
        byte[] head = zData.getHead();

        List<byte[]> dels = new ArrayList<>();
        final RocksIterator iterator = db.rocksDB().newIterator();
        iterator.seek(seek);
        long index = 0;
        while (iterator.isValid() && index <= end) {
            byte[] key_bs = iterator.key();
            if (!BytesUtil.checkHead(head, key_bs)) break;
            ZDataD zDataD = ZDataD.build(key_bs);
            ZData key = zDataD.convertValue();
            index = key.getScore();
            if (index > end) {
                break;
            }
            entries.add(new Entry(index, key.value));

            //DEL
            metaV.setSize(metaV.getSize() - 1);
            dels.add(zDataD.toBytes());
            SDataD sDataD = new SDataD(zDataD.getMapKeySize(), key_b, zDataD.getVersion(), zDataD.getValue());
            dels.add(sDataD.toBytes());

            iterator.next();
        }

        db.start();
        try {
            removeDo(metaV, dels);
            db.commit();
        } finally {
            db.release();
        }
        return entries;
    }

    private void removeDo(MetaV metaV, List<byte[]> dels) throws RocksDBException {
        db.put(key_b, metaV.convertMetaBytes().toBytes());
        for (byte[] del : dels) {
            db.delete(del);
        }
    }


    /**
     * 对指定成员的分数加上增量 increment
     *
     * @param increment
     * @param vs
     * @throws Exception
     */
    private synchronized void incrby(int increment, byte[]... vs) throws Exception {
        db.start();
        try {
            MetaV metaV = getMetaV();
            for (byte[] v : vs) {
                SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
                SDataD sDataD = sData.convertBytes();
                byte[] scoreD = db.rocksDB().get(sDataD.toBytes());
                if (scoreD != null) {
                    int score = ArrayKits.bytesToInt(scoreD, 0) + increment;
                    scoreD = ArrayKits.intToBytes(score);
                    ZDataD zDataD = new ZDataD(sDataD.getMapKeySize(), sDataD.getMapKey(), sDataD.getVersion(), scoreD, sDataD.getValue());
                    db.put(sData.convertBytes().toBytes(), scoreD);
                    db.put(zDataD.toBytes(), null);
                }
            }
            db.commit();
        } finally {
            db.release();
        }
    }

    /**
     * 删除指定成员
     *
     * @param vs
     * @throws Exception
     */
    public synchronized void remove(byte[]... vs) throws Exception {

        MetaV metaV = getMetaV();
        List<byte[]> dels = new ArrayList<>();
        for (byte[] v : vs) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
            SDataD sDataD = sData.convertBytes();
            byte[] scoreD = db.rocksDB().get(sDataD.toBytes());
            if (scoreD != null) {
                ZDataD zDataD = new ZDataD(sDataD.getMapKeySize(), sDataD.getMapKey(), sDataD.getVersion(), scoreD, sDataD.getValue());
                dels.add(zDataD.toBytes());
                dels.add(sDataD.toBytes());
                metaV.setSize(metaV.getSize() - 1);
            }
        }
        db.start();
        try {
            removeDo(metaV, dels);
            db.commit();
        } catch (Exception e) {
            db.release();
        }
    }

    /**
     * 返回成员的分数值,如成员不存在，List对应位置则为null
     *
     * @param vs
     * @return
     * @throws Exception
     */
    public List<Long> score(byte[]... vs) throws Exception {
        MetaV metaV = getMetaV();
        List<Long> scores = new ArrayList<>();
        for (byte[] v : vs) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
            byte[] scoreD = db.rocksDB().get(sData.convertBytes().toBytes());
            if (scoreD != null) {
                scores.add(ArrayKits.bytesToLong(scoreD));
            }
            scores.add(null);
        }
        return scores;
    }

    /**
     * 返回成员的分数值
     *
     * @param v
     * @return
     * @throws Exception
     */
    public Long score(byte[] v) throws Exception {
        MetaV metaV = getMetaV();
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
        byte[] scoreD = db.rocksDB().get(sData.convertBytes().toBytes());
        if (scoreD != null) {
            return ArrayKits.bytesToLong(scoreD);
        }
        return null;
    }

    public ZSet(DB db, String key) {
        this.db = db;
        this.key_b = ArrayKits.addAll(HEAD_B, key.getBytes(charset));
    }

    private MetaV addCheck(byte[] k_v) throws RocksDBException {
        MetaV metaV = null;
        if (k_v != null) {
            MetaD metaD = ZSet.MetaD.build(k_v);
            metaV = metaD.convertMetaV();
            long nowTime = System.currentTimeMillis() / 1000;
            if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
                metaV = null;
                db.rocksDB().put(ArrayKits.addAll("D".getBytes(), key_b, metaD.getVersion()), metaD.toBytes());
            }
        }
        return metaV;
    }

    private MetaV getMetaV() throws Exception {
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

    @Override
    public synchronized void delete() throws Exception {
        MetaV metaV = getMetaV();
        MetaD metaVD = metaV.convertMetaBytes();
        db.rocksDB().put(ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion()), metaVD.toBytes());
        db.rocksDB().delete(key_b);

        List<byte[]> delete_keys = new ArrayList<>();
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), null);
        byte[] head = sData.getHead();

        try (final RocksIterator iterator = this.db.rocksDB().newIterator()) {
            iterator.seek(head);

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (!BytesUtil.checkHead(head, key)) break;
                SDataD sDataD = SDataD.build(key);
                delete_keys.add(key);
                ZDataD zDataD = new ZDataD(sDataD.getMapKeySize(), sDataD.getMapKey(), sDataD.getVersion(), iterator.key(), sDataD.getValue());
                delete_keys.add(zDataD.toBytes());
                iterator.next();

                if (delete_keys.size() >= 100000) {
                    final WriteBatch batch = new WriteBatch();
                    for (byte[] delete_key : delete_keys) {
                        batch.delete(delete_key);
                    }
                    db.rocksDB().write(db.writeOptions(), batch);
                    delete_keys.clear();
                }
            }
        }
        final WriteBatch batch = new WriteBatch();
        for (byte[] delete_key : delete_keys) {
            batch.delete(delete_key);
        }
        db.rocksDB().write(db.writeOptions(), batch);
        db.rocksDB().delete(ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion()));
    }

    @Override
    public synchronized void deleteFast() throws Exception {
        db.start();
        try {
            MetaV metaV = getMetaV();
            MetaD metaVD = metaV.convertMetaBytes();
            db.put(ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion()), metaVD.toBytes());
            db.delete(key_b);
            db.commit();
        } finally {
            db.release();
        }
    }

    @Override
    public synchronized int getTtl() throws Exception {
        MetaV metaV = getMetaV();
        return (int) (System.currentTimeMillis() / 1000 - metaV.getTimestamp());
    }

    @Override
    public synchronized void delTtl() throws Exception {
        MetaV metaV = getMetaV();
        metaV.setTimestamp(-1);
        db.start();
        db.put(key_b, metaV.convertMetaBytes().toBytes());
        db.ttlZset().remove(metaV.convertMetaBytes().toBytes());
        db.commit();
    }

    @Override
    public void ttl(int ttl) throws Exception {
        MetaV metaV = getMetaV();
        db.start();
        metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
        db.put(key_b, metaV.convertMetaBytes().toBytes());
        db.ttlZset().add(metaV.convertMetaBytes().toBytes(), metaV.getTimestamp());
        db.commit();

    }

    @Override
    public boolean isExist() throws RocksDBException {
        byte[] k_v = db.rocksDB().get(this.key_b);
        MetaV meta = addCheck(k_v);
        return meta != null;
    }

    @Override
    public int size() throws Exception {
        MetaV metaV = getMetaV();
        return metaV.getSize();
    }

    @Override
    public Entry getEntry(RocksIterator iterator) {
        byte[] key_bs = iterator.key();
        if (key_bs == null) {
            return null;
        }
        SData sData = SDataD.build(key_bs).convertValue();
        Entry entry = new Entry(ArrayKits.bytesToLong(iterator.value()), sData.value);
        return entry;
    }

    @Data
    @AllArgsConstructor
    public static class Entry extends RCollection.Entry {
        public long score;
        public byte[] value;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MetaV {
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
            sDataD.setValue(ArrayUtil.sub(bytes, position, bytes.length - 1));
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

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ZData {
        private int mapKeySize;
        private byte[] mapKey;
        private int version;
        private long score;
        private byte[] value;

        public ZDataD convertBytes() {
            ZDataD zDataD = new ZDataD();
            zDataD.setMapKeySize(ArrayKits.intToBytes(this.mapKeySize));
            zDataD.setMapKey(this.mapKey);
            zDataD.setVersion(ArrayKits.intToBytes(this.version));
            zDataD.setScore(ArrayKits.longToBytes(this.score));
            zDataD.setValue(this.value);
            return zDataD;
        }


        public byte[] getSeek() {
            byte[] value = ArrayKits.addAll(HEAD_SCORE_B, ArrayKits.intToBytes(this.mapKeySize),
                    this.mapKey, ArrayKits.intToBytes(this.version), ArrayKits.longToBytes(this.score));
            return value;
        }

        public byte[] getHead() {
            byte[] value = ArrayKits.addAll(HEAD_SCORE_B, ArrayKits.intToBytes(this.mapKeySize),
                    this.mapKey, ArrayKits.intToBytes(this.version));
            return value;
        }

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ZDataD {
        private byte[] mapKeySize;
        private byte[] mapKey;
        private byte[] version;
        private byte[] score;
        private byte[] value;


        public byte[] toBytes() {
            return ArrayKits.addAll(HEAD_SCORE_B, this.mapKeySize, this.mapKey, this.version, this.score, this.value);
            /*return String.join("",new String(HEAD_SCORE_B), ArrayKits.bytesToInt(this.mapKeySize,0)+"", new String(this.mapKey)
                    , ArrayKits.bytesToInt(this.mapKeySize,0)+"",ArrayKits.bytesToLong(this.score)+"", new String(this.value)).getBytes();*/
        }

        public static ZDataD build(byte[] bytes) {
            ZDataD zData = new ZDataD();
            zData.setMapKeySize(ArrayUtil.sub(bytes, 1, 5));
            int position = ArrayKits.bytesToInt(zData.getMapKeySize(), 0);
            zData.setMapKey(ArrayUtil.sub(bytes, 5, position = 5 + position));
            zData.setVersion(ArrayUtil.sub(bytes, position, position = position + 4));
            zData.setScore(ArrayUtil.sub(bytes, position, position = position + 8));
            zData.setValue(ArrayUtil.sub(bytes, position, bytes.length - 1));
            return zData;
        }


        public ZData convertValue() {
            ZData zData = new ZData();
            zData.setMapKeySize(ArrayKits.bytesToInt(this.mapKeySize, 0));
            zData.setMapKey(this.mapKey);
            zData.setVersion(ArrayKits.bytesToInt(this.version, 0));
            zData.setScore(ArrayKits.bytesToLong(this.score));
            zData.setValue(this.value);
            return zData;
        }

    }
}
