package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.base.*;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.KitDBException;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.List;

public class RSet extends RCollection {
    public final static String HEAD = KeyEnum.SET.getKey();

    public final static byte[] HEAD_B = HEAD.getBytes();
    public final static byte[] HEAD_V_B = KeyEnum.SET_V.getBytes();

    @Override
    protected TxLock getTxLock(String key) {
        return new TxLock(String.join(":", HEAD, key));
    }

    protected RSet(DB db) {
        super(db, false, 128);
    }

    protected byte[] getKey(String key) throws KitDBException {
        DAssert.notNull(key, ErrorType.NULL, "Key is null");
        return ArrayKits.addAll(HEAD_B, key.getBytes(charset));
    }


    /**
     * 移除并返回集合中的n个元素
     *
     * @param num
     * @return
     * @throws KitDBException
     */
    public List<byte[]> pop(String key, int num) throws KitDBException {
        LockEntity lockEntity = lock.lock(key);

        List<byte[]> values = new ArrayList<>();
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
            start();
            List<byte[]> dels = new ArrayList<>();
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), ArrayKits.intToBytes(0));
            byte[] head = sData.getHead();
            iterator.seek(head);
            int count = 0;
            while (iterator.isValid() && count++ < num) {
                byte[] key_bs = iterator.key();
                if (!BytesUtil.checkHead(head, key_bs)) return values;
                SDataD sDataD = SDataD.build(key_bs);
                values.add(sDataD.getValue());
                dels.add(key_bs);
                iterator.next();
            }
            try {
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                removeDo(metaV, dels);
                commit();
            } finally {
                lock.unlock(lockEntity);
                release();
            }
            return values;
        }
    }

    /**
     * 移除集合中一个或多个成员
     *
     * @param values
     * @throws KitDBException
     */
    public void remove(String key, byte[]... values) throws KitDBException {
        DAssert.notEmpty(values, ErrorType.EMPTY, "values is empty");
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        List<byte[]> dels = new ArrayList<>();
        for (byte[] v : values) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
            SDataD sDataD = sData.convertBytes();
            byte[] scoreD = getDB(sDataD.toBytes(), SstColumnFamily.DEFAULT);
            if (scoreD != null) {
                dels.add(sDataD.toBytes());
            }
        }
        try {
            start();
            removeDo(metaV, dels);
            putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            commit();
        } catch (Exception e) {
            lock.unlock(lockEntity);
            release();
        }
    }

    public boolean isMember(String key, byte[] value) throws KitDBException {
        byte[] key_b = getKey(key);
        byte[] k_v = getDB(key_b, SstColumnFamily.META);
        MetaV metaV = addCheck(key_b, k_v);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), value);
        return getDB(sData.convertBytes().toBytes(), SstColumnFamily.DEFAULT) != null;
    }

    /**
     * 向集合添加一个或多个成员,如果集合在添加时不存在，则为集合设置TTL
     *
     * @param ttl
     * @param values
     * @throws KitDBException
     */
    public void addMayTTL(String key, int ttl, byte[]... values) throws KitDBException {
        DAssert.notEmpty(values, ErrorType.EMPTY, "values is empty");
        DAssert.isTrue(ArrayKits.noRepeate(values), ErrorType.REPEATED_KEY, "Repeated memebers");
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        start();
        try {
            byte[] k_v = getDB(key_b, SstColumnFamily.META);
            MetaV metaV = addCheck(key_b, k_v);
            if (metaV != null) {
                setEntry(key_b, metaV, values);
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
            } else {
                if (ttl != -1) {
                    ttl = (int) (System.currentTimeMillis() / 1000 + ttl);
                }
                metaV = new MetaV(0, ttl, db.versionSequence().incr());
                setEntry(key_b, metaV, values);
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

    /**
     * 向集合添加一个或多个成员
     *
     * @param values
     * @throws KitDBException
     */
    public synchronized void add(String key, byte[]... values) throws KitDBException {

        addMayTTL(key, -1, values);
    }


    @Override
    public void delete(String key) throws KitDBException {
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
    public KeyIterator getKeyIterator() throws KitDBException {
        return null;
    }

    public void deleteFast(String key) throws KitDBException {
        LockEntity lockEntity = lock.lock(key);

        byte[] key_b = getKey(key);
        byte[] k_v = getDB(key_b, SstColumnFamily.META);
        if (k_v == null) {
            return;
        }
        MetaV meta = MetaD.build(k_v).convertMetaV();
        try {
            deleteFast(key_b, meta);
        } finally {
            lock.unlock(lockEntity);
        }
    }

    @Override
    public RIterator<RSet> iterator(String key) throws KitDBException {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), "".getBytes());
        RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT);
        iterator.seek(sData.getHead());
        return new RIterator<>(iterator, this, sData.getHead());
    }


    @Override
    public int getTtl(String key) throws KitDBException {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
        if (metaV.getTimestamp() == -1) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - metaV.getTimestamp());
    }

    @Override
    public void delTtl(String key) throws KitDBException {
        LockEntity lockEntity = lock.lock(key);
        try {
            byte[] key_b = getKey(key);
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
    public void ttl(String key, int ttl) throws KitDBException {
        LockEntity lockEntity = lock.lock(key);
        try {
            byte[] key_b = getKey(key);
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
    public boolean isExist(String key) throws KitDBException {
        byte[] key_b = getKey(key);
        byte[] k_v = getDB(key_b, SstColumnFamily.META);
        MetaV meta = addCheck(key_b, k_v);
        return meta != null;
    }

    @Override
    public int size(String key) throws KitDBException {
        byte[] key_b = getKey(key);
        MetaV metaV = getMeta(key_b);
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

    private MetaV addCheck(byte[] key_b, byte[] k_v) {
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

    private void removeDo(MetaV metaV, List<byte[]> dels) {
        for (byte[] del : dels) {
            metaV.setSize(metaV.getSize() - 1);
            deleteDB(del, SstColumnFamily.DEFAULT);
        }
    }


    private void setEntry(byte[] key_b, MetaV metaV, byte[][] values) throws KitDBException {
        for (byte[] value : values) {
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), value);
            byte[] member = sData.convertBytes().toBytes();
            if (getDB(member, SstColumnFamily.DEFAULT) == null) {
                metaV.size = metaV.size + 1;
            }
            putDB(sData.convertBytes().toBytes(), "".getBytes(), SstColumnFamily.DEFAULT);
        }
    }


    private void delete(byte[] key_b, MetaD metaD) {
        MetaV metaV = metaD.convertMetaV();
        deleteDB(key_b, SstColumnFamily.META);
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), null);
        deleteHead(sData.getHead(), SstColumnFamily.DEFAULT);
        deleteDB(ArrayKits.addAll("D".getBytes(charset), key_b, metaD.getVersion()), SstColumnFamily.DEFAULT);
    }

    @Override
    protected MetaV getMeta(byte[] key_b) throws KitDBException {
        byte[] k_v = this.getDB(key_b, SstColumnFamily.META);
        if (k_v == null) {
            throw new KitDBException(ErrorType.NOT_EXIST, "Set do not exist");
        }
        MetaV metaV = MetaD.build(k_v).convertMetaV();
        long nowTime = System.currentTimeMillis();
        if (metaV.getTimestamp() != -1 && nowTime > metaV.getTimestamp()) {
            throw new KitDBException(ErrorType.NOT_EXIST, "Set do not exist");
        }
        return metaV;
    }

    @Data
    @AllArgsConstructor
    public static class Entry extends REntry {
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
