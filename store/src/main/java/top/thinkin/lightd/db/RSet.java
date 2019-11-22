package top.thinkin.lightd.db;


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
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock(key);
            try {
                List<byte[]> values = new ArrayList<>();
                byte[] key_b = getKey(key);
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return values;
                }
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

                    removeDo(metaV, dels);
                    putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                    commit();

                    checkTxCommit();
                    return values;
                }
            } finally {
                unlock(lockEntity);
                release();
            }
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
        }
    }

    /**
     * 移除集合中一个或多个成员
     *
     * @param values
     * @throws KitDBException
     */
    public void remove(String key, byte[]... values) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            DAssert.notEmpty(values, ErrorType.EMPTY, "values is empty");
            LockEntity lockEntity = lock(key);
            try {
                byte[] key_b = getKey(key);
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return;
                }
                List<byte[]> dels = new ArrayList<>();
                for (byte[] v : values) {
                    SData sData = new SData(key_b.length, key_b, metaV.getVersion(), v);
                    SDataD sDataD = sData.convertBytes();
                    byte[] scoreD = getDB(sDataD.toBytes(), SstColumnFamily.DEFAULT);
                    if (scoreD != null) {
                        dels.add(sDataD.toBytes());
                    }
                }

                start();
                removeDo(metaV, dels);
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                commit();
            } catch (Exception e) {
                unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
        }
    }

    public boolean contains(String key, byte[] value) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return false;
            }
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), value);
            return getDB(sData.convertBytes().toBytes(), SstColumnFamily.DEFAULT) != null;
        }
    }

    /**
     * 向集合添加一个或多个成员,如果集合在添加时不存在，则为集合设置TTL
     *
     * @param ttl
     * @param values
     * @throws KitDBException
     */
    public void addMayTTL(String key, int ttl, byte[]... values) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            DAssert.notEmpty(values, ErrorType.EMPTY, "values is empty");
            DAssert.isTrue(ArrayKits.noRepeate(values), ErrorType.REPEATED_KEY, "Repeated memebers");
            byte[] key_b = getKey(key);

            LockEntity lockEntity = lock(key);
            try {
                start();
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

                    if (metaV.getTimestamp() != -1) {
                        setTimerCollection(KeyEnum.COLLECT_TIMER,
                                metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                    }
                }
                commit();
            } finally {
                unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
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


    protected synchronized void deleteByClear(byte[] key_b, MetaD meta) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            start();
            delete(key_b, meta);
            commitLocal();
        } finally {
            release();
        }
    }

    protected void deleteTTL(int time, byte[] key_b, byte[] meta_b) throws KitDBException {
        String key = new String(ArrayKits.sub(key_b, 1, key_b.length + 1), charset);
        LockEntity lockEntity = lock(key);
        try (CloseLock ignored = checkClose()) {
            MetaV metaV = getMetaP(key_b);
            if (metaV != null && time != metaV.timestamp) {
                return;
            }
            deleteTTL(key_b, MetaD.build(meta_b).convertMetaV(), metaV.version);
        } finally {
            unlock(lockEntity);
        }
    }


    @Override
    public void delete(String key) throws KitDBException {
        checkTxRange();
        LockEntity lockEntity = lock(key);
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            try {
                start();
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return;
                }
                deleteDB(key_b, SstColumnFamily.META);
                delete(key_b, metaV.convertMetaBytes());
                commit();
            } finally {
                unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
        }
    }

    @Override
    public KeyIterator getKeyIterator() throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            return getKeyIterator(HEAD_B);
        }
    }

    public void deleteFast(String key) throws KitDBException {
        checkTxStart();
        LockEntity lockEntity = lock(key);
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            byte[] k_v = getDB(key_b, SstColumnFamily.META);
            if (k_v == null) {
                checkTxCommit();
                return;
            }
            MetaV meta = MetaD.build(k_v).convertMetaV();
            try {
                deleteFast(key_b, meta);
            } finally {
                unlock(lockEntity);
            }
            checkTxCommit();
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RIterator<RSet> iterator(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return null;
            }
            SData sData = new SData(key_b.length, key_b, metaV.getVersion(), "".getBytes());
            RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT);
            iterator.seek(sData.getHead());
            return new RIterator<>(iterator, this, sData.getHead());
        }
    }


    @Override
    public int getTtl(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return -1;
            }
            if (metaV.getTimestamp() == -1) {
                return -1;
            }
            return (int) (metaV.getTimestamp() - System.currentTimeMillis() / 1000);
        }
    }

    @Override
    public void delTtl(String key) throws KitDBException {
        checkTxStart();
        LockEntity lockEntity = lock(key);
        try (CloseLock ignored = checkClose()) {
            try {
                byte[] key_b = getKey(key);
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return;
                }
                start();
                delTimerCollection(KeyEnum.COLLECT_TIMER,
                        metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                metaV.setTimestamp(-1);
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                commit();
            } finally {
                unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
        }
    }

    @Override
    public void ttl(String key, int ttl) throws KitDBException {
        checkTxStart();
        LockEntity lockEntity = lock(key);
        try (CloseLock ignored = checkClose()) {
            try {
                byte[] key_b = getKey(key);
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return;
                }
                start();
                delTimerCollection(KeyEnum.COLLECT_TIMER,
                        metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                setTimerCollection(KeyEnum.COLLECT_TIMER,
                        metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                commit();
            } finally {
                unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
        }
    }

    @Override
    public boolean isExist(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            return metaV != null;
        }
    }

    @Override
    public int size(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            return metaV.getSize();
        }
    }

    @Override
    public Entry getEntry(RocksIterator iterator) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_bs = iterator.key();
            if (key_bs == null) {
                return null;
            }
            SData sData = SDataD.build(key_bs).convertValue();
            Entry entry = new Entry(sData.value);
            return entry;
        }
    }

    // TODO
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
        SData sData = new SData(key_b.length, key_b, metaV.getVersion(), null);
        deleteHead(sData.getHead(), SstColumnFamily.DEFAULT);
        deleteDB(ArrayKits.addAll("D".getBytes(charset), key_b, metaD.getVersion()), SstColumnFamily.DEFAULT);
    }


    private MetaV getMetaP(byte[] key_b) throws KitDBException {
        byte[] k_v = this.getDB(key_b, SstColumnFamily.META);
        if (k_v == null) return null;
        MetaV metaV = MetaD.build(k_v).convertMetaV();
        return metaV;
    }

    @Override
    protected MetaV getMeta(byte[] key_b) throws KitDBException {
        MetaV metaV = getMetaP(key_b);
        if (metaV == null) {
            return null;
        }
        if (metaV.getTimestamp() != -1 && (System.currentTimeMillis() / 1000) - metaV.getTimestamp() >= 0) {
            metaV = null;
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
            metaD.setSize(ArrayKits.sub(bytes, 1, 5));
            metaD.setTimestamp(ArrayKits.sub(bytes, 5, 9));
            metaD.setVersion(ArrayKits.sub(bytes, 9, 13));
            return metaD;
        }


        public byte[] toBytesHead() {
            byte[] value = ArrayKits.addAll(HEAD_B, ArrayKits.intToBytes(0),
                    ArrayKits.intToBytes(0), this.version);
            return value;
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
            sDataD.setMapKeySize(ArrayKits.sub(bytes, 1, 5));
            int position = ArrayKits.bytesToInt(sDataD.getMapKeySize(), 0);
            sDataD.setMapKey(ArrayKits.sub(bytes, 5, position = 5 + position));
            sDataD.setVersion(ArrayKits.sub(bytes, position, position = position + 4));
            sDataD.setValue(ArrayKits.sub(bytes, position, bytes.length));
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
