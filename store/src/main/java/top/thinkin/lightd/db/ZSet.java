package top.thinkin.lightd.db;

import cn.hutool.cache.CacheUtil;
import cn.hutool.cache.impl.TimedCache;
import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.*;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.LightDException;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ZSet extends RCollection {
    public static String HEAD = KeyEnum.ZSET.getKey();
    public static byte[] HEAD_B = HEAD.getBytes();
    public static byte[] HEAD_SCORE_B = KeyEnum.ZSET_S.getBytes();
    public static byte[] HEAD_V_B = KeyEnum.ZSET_V.getBytes();

    @Override
    protected TxLock getTxLock(String key) {
        return new TxLock(String.join(":", HEAD, key));
    }

    TimedCache<String, String> timedCache = CacheUtil.newTimedCache(4);


    protected ZSet(DB db) {
        super(db, false, 128);
    }


    protected byte[] getKey(String key) throws LightDException {
        DAssert.notNull(key, ErrorType.NULL, "Key is null");
        return ArrayKits.addAll(HEAD_B, key.getBytes(charset));
    }

    public synchronized void add(String key, byte[] v, long score) throws Exception {
        addMayTTL(key, -1, new Entry(score, v));
    }

    public synchronized void add(String key, Entry... entrys) throws Exception {
        addMayTTL(key, -1, entrys);
    }

    public synchronized void addMayTTL(String key, int ttl, byte[] v, long score) throws Exception {
        addMayTTL(key, ttl, new Entry(score, v));
    }

    public synchronized void addMayTTL(final String key, int ttl, Entry... entrys) throws Exception {
        DAssert.notEmpty(entrys, ErrorType.EMPTY, "entrys is empty");
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);

        byte[][] bytess = new byte[entrys.length][];
        for (int i = 0; i < entrys.length; i++) {
            bytess[i] = entrys[i].value;
        }
        DAssert.isTrue(ArrayKits.noRepeate(bytess), ErrorType.REPEATED_KEY, "Repeated memebers");
        try {
            start();
            byte[] k_v = getDB(key_b, SstColumnFamily.META);
            MetaV metaV = addCheck(key_b, k_v);
            if (metaV != null) {
                setEntry(key_b, metaV, entrys);
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new MetaV(0, ttl, db.versionSequence().incr());
                setEntry(key_b, metaV, entrys);
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            }
            if (metaV.getTimestamp() != -1) {
                setTimer(KeyEnum.COLLECT_TIMER, metaV.getTimestamp(), metaV.convertMetaBytes().toBytes());
            }

            commit();

        } finally {
            lock.unlock(lockEntity);
            release();
        }
    }


    private void setEntry(byte[] key_b, MetaV metaV, Entry[] entrys) throws RocksDBException {
        for (Entry entry : entrys) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), entry.value);
            ZData zData = new ZData(key_b.length, key_b, metaV.getVersion(), entry.score, entry.value);
            byte[] member = sData.convertBytes().toBytes();
            //if (getDB(member, SstColumnFamily.DEFAULT) == null) {
            metaV.size = metaV.size + 1;
            //}
            putDB(member, ArrayKits.longToBytes(entry.score), SstColumnFamily.DEFAULT);
            putDB(zData.convertBytes().toBytes(), "".getBytes(), SstColumnFamily.DEFAULT);
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
    public List<Entry> range(String key, long start, long end, int limit) throws Exception {
        byte[] key_b = getKey(key);

        List<Entry> entries = new ArrayList<>();
        MetaV metaV = getMeta(key_b);
        ZData zData = new ZData(key_b.length, key_b, metaV.getVersion(), start, "".getBytes());

        byte[] seek = zData.getSeek();
        byte[] head = zData.getHead();
        int count = 0;
        try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
            iterator.seek(seek);
            long index = 0;
            while (iterator.isValid() && index <= end && count < limit) {
                byte[] key_bs = iterator.key();
                if (!BytesUtil.checkHead(head, key_bs)) break;
                ZData izData = ZDataD.build(key_bs).convertValue();
                index = izData.getScore();
                if (index > end) {
                    break;
                }
                entries.add(new Entry(index, izData.value));
                count++;
                iterator.next();
            }
        }
        return entries;
    }

    /**
     * 返回指定区间分数的成员并删除
     *
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public synchronized List<Entry> rangeDel(String key, long start, long end, int limit) throws Exception {
        byte[] key_b = getKey(key);
        LockEntity lockEntity = lock.lock(key);
        List<Entry> entries = new ArrayList<>();
        try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return entries;
            }
            ZData zData = new ZData(key_b.length, key_b, metaV.getVersion(), start, "".getBytes());

            byte[] seek = zData.getSeek();
            byte[] head = zData.getHead();

            List<byte[]> dels = new ArrayList<>();
            iterator.seek(seek);
            long index = 0;
            int count = 0;
            while (iterator.isValid() && index <= end && count < limit) {
                byte[] key_bs = iterator.key();
                if (!BytesUtil.checkHead(head, key_bs)) break;
                ZDataD zDataD = ZDataD.build(key_bs);
                ZData izData = zDataD.convertValue();
                index = izData.getScore();
                if (index > end) {
                    break;
                }
                entries.add(new Entry(index, izData.value));
                count++;
                //DEL
                metaV.setSize(metaV.getSize() - 1);
                dels.add(zDataD.toBytes());
                SDataD sDataD = new SDataD(zDataD.getMapKeySize(), key_b, zDataD.getVersion(), zDataD.getValue());
                dels.add(sDataD.toBytes());
                iterator.next();
            }
            start();
            removeDo(key_b, metaV, dels);
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            commit();
        } finally {
            lock.unlock(lockEntity);
            release();
        }
        return entries;
    }


    @Override
    public RIterator<ZSet> iterator(String key) throws Exception {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), "".getBytes());
        RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT);
        iterator.seek(sData.getHead());
        RIterator<ZSet> rIterator = new RIterator<>(iterator, this, sData.getHead());
        return rIterator;
    }




    private void removeDo(byte[] key_b, MetaV metaV, List<byte[]> dels) {
        for (byte[] del : dels) {
            deleteDB(del, SstColumnFamily.DEFAULT);
        }
    }


    /**
     * 对指定成员的分数加上增量 increment
     *
     * @param increment
     * @param vs
     * @throws Exception
     */
    private synchronized void incrby(String key, int increment, byte[]... vs) throws Exception {
        DAssert.notEmpty(vs, ErrorType.EMPTY, "vs is empty");
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        try {
            start();
            MetaV metaV = getMeta(key_b);
            for (byte[] v : vs) {
                SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
                SDataD sDataD = sData.convertBytes();
                byte[] scoreD = getDB(sDataD.toBytes(), SstColumnFamily.DEFAULT);
                if (scoreD != null) {
                    int score = ArrayKits.bytesToInt(scoreD, 0) + increment;
                    scoreD = ArrayKits.intToBytes(score);
                    ZDataD zDataD = new ZDataD(sDataD.getMapKeySize(), sDataD.getMapKey(), sDataD.getVersion(), scoreD, sDataD.getValue());
                    putDB(sData.convertBytes().toBytes(), scoreD, SstColumnFamily.DEFAULT);
                    putDB(zDataD.toBytes(), null, SstColumnFamily.DEFAULT);
                }
            }
            commit();
        } finally {
            lock.unlock(lockEntity);
            release();
        }
    }

    /**
     * 删除指定成员
     *
     * @param vs
     * @throws Exception
     */
    public synchronized void remove(String key, byte[]... vs) throws Exception {
        DAssert.notEmpty(vs, ErrorType.EMPTY, "vs is empty");
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        start();
        try {
            MetaV metaV = getMeta(key_b);
            List<byte[]> dels = new ArrayList<>();
            for (byte[] v : vs) {
                SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
                SDataD sDataD = sData.convertBytes();
                byte[] scoreD = getDB(sDataD.toBytes(), SstColumnFamily.DEFAULT);
                if (scoreD != null) {
                    ZDataD zDataD = new ZDataD(sDataD.getMapKeySize(), sDataD.getMapKey(), sDataD.getVersion(), scoreD, sDataD.getValue());
                    dels.add(zDataD.toBytes());
                    dels.add(sDataD.toBytes());
                    metaV.setSize(metaV.getSize() - 1);
                }
            }
            removeDo(key_b, metaV, dels);
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            commit();
        } finally {
            lock.unlock(lockEntity);
            release();
        }
    }


    /**
     * 返回成员的分数值,如成员不存在，List对应位置则为null
     *
     * @param key
     * @param vs
     * @return
     * @throws Exception
     */
    public List<Long> score(String key, byte[]... vs) throws Exception {
        DAssert.notEmpty(vs, ErrorType.EMPTY, "vs is empty");
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        List<Long> scores = new ArrayList<>();
        for (byte[] v : vs) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
            byte[] scoreD = getDB(sData.convertBytes().toBytes(), SstColumnFamily.DEFAULT);
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
    public Long score(String key, byte[] v) throws Exception {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
        byte[] scoreD = getDB(sData.convertBytes().toBytes(), SstColumnFamily.DEFAULT);
        if (scoreD != null) {
            return ArrayKits.bytesToLong(scoreD);
        }
        return null;
    }


    private MetaV addCheck(byte[] key_b, byte[] k_v) throws RocksDBException {
        MetaV metaV = null;
        if (k_v != null) {
            MetaD metaD = MetaD.build(k_v);
            metaV = metaD.convertMetaV();
            long nowTime = System.currentTimeMillis() / 1000;
            if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
                metaV = null;
            }
        }
        return metaV;
    }

    @Override
    protected MetaV getMeta(byte[] key_b) throws Exception {
        byte[] k_v = this.getDB(key_b, SstColumnFamily.META);
        if (k_v == null) {
            return null;
        }
        MetaV metaV = MetaD.build(k_v).convertMetaV();
        long nowTime = System.currentTimeMillis();
        if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
            return null;
        }
        return metaV;
    }


    private void delete(byte[] key_b, MetaD metaD) {
        MetaV metaV = metaD.convertMetaV();
        deleteDB(key_b, SstColumnFamily.META);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), null);
        deleteHead(sData.getHead(), SstColumnFamily.DEFAULT);
        ZData zData = new ZData(sData.getMapKeySize(), sData.getMapKey(), sData.getVersion(), 0, null);
        deleteHead(zData.getHead(), SstColumnFamily.DEFAULT);
        deleteDB(ArrayKits.addAll("D".getBytes(charset), key_b, metaD.getVersion()), SstColumnFamily.DEFAULT);
    }


    @Override
    public synchronized void delete(String key) throws Exception {
        LockEntity lockEntity = lock.lock(key);
        byte[] key_b = getKey(key);
        try {
            start();
            MetaV metaV = getMeta(key_b);
            delete(key_b, metaV.convertMetaBytes());
            commit();
        } finally {
            lock.unlock(lockEntity);

            release();
        }
    }

    @Override
    public KeyIterator getKeyIterator() throws Exception {
        return getKeyIterator(HEAD_B);
    }


    public synchronized void deleteFast(String key) throws Exception {
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        try {
            deleteFast(key_b, metaV);
        } finally {
            lock.unlock(lockEntity);
        }
    }

    @Override
    public int getTtl(String key) throws Exception {
        byte[] key_b = getKey(key);

        MetaV metaV = getMeta(key_b);
        if (metaV.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - metaV.getTimestamp());
    }

    @Override
    public synchronized void delTtl(String key) throws Exception {
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        try {
            MetaV metaV = getMeta(key_b);
            metaV.setTimestamp(-1);
            start();
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            delTimer(KeyEnum.COLLECT_TIMER, metaV.getTimestamp(), metaV.convertMetaBytes().toBytes());

            commit();
        } finally {
            lock.unlock(lockEntity);
            release();
        }
    }

    @Override
    public void ttl(String key, int ttl) throws Exception {
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        try {
            MetaV metaV = getMeta(key_b);
            start();
            delTimer(KeyEnum.COLLECT_TIMER, metaV.getTimestamp(), metaV.convertMetaBytes().toBytes());
            metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            setTimer(KeyEnum.COLLECT_TIMER, metaV.getTimestamp(), metaV.convertMetaBytes().toBytes());
            commit();
        } finally {
            lock.unlock(lockEntity);
            release();
        }

    }

    @Override
    public boolean isExist(String key) throws RocksDBException, LightDException {
        byte[] key_b = getKey(key);
        byte[] k_v = getDB(key_b, SstColumnFamily.META);
        MetaV meta = addCheck(key_b, k_v);
        return meta != null;
    }

    @Override
    public int size(String key) throws Exception {
        int size = 0;
        try (RIterator<ZSet> iterator = iterator(key)) {
            while (iterator.hasNext()) {
                iterator.next();
                size++;
            }
        }
        return size;
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
    public static class Entry extends REntry {
        private long score;
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
        }

        public static ZDataD build(byte[] bytes) {
            ZDataD zData = new ZDataD();
            zData.setMapKeySize(ArrayUtil.sub(bytes, 1, 5));
            int position = ArrayKits.bytesToInt(zData.getMapKeySize(), 0);
            zData.setMapKey(ArrayUtil.sub(bytes, 5, position = 5 + position));
            zData.setVersion(ArrayUtil.sub(bytes, position, position = position + 4));
            zData.setScore(ArrayUtil.sub(bytes, position, position = position + 8));
            zData.setValue(ArrayUtil.sub(bytes, position, bytes.length));
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
