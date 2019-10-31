package top.thinkin.lightd.db;

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
import top.thinkin.lightd.exception.KitDBException;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * A List
 */
@Slf4j
public class RList extends RCollection {

    public final static String HEAD = KeyEnum.LIST.getKey();
    public final static byte[] HEAD_VALUE_B = KeyEnum.LIST_VALUE.getBytes();
    public final static byte[] HEAD_B = HEAD.getBytes();

    @Override
    protected TxLock getTxLock(String key) {
        return new TxLock(String.join(":", HEAD, key));
    }

    protected RList(DB db) {
        super(db, false, 128);
    }


    public void add(String key, byte[] v) throws KitDBException {
        addMayTTLPrivate(key, v, -1);
    }


    public void addAll(String key, List<byte[]> vs) throws KitDBException {
        addAllMayTTLPrivate(key, vs, -1);
    }


    public void addAllMayTTL(String key, List<byte[]> vs, int ttl) throws KitDBException {
        DAssert.isTrue(ttl > 0, ErrorType.PARAM_ERROR, "ttl must greater than 0");
        addAllMayTTLPrivate(key, vs, ttl);
    }

    public void set(String key, long i, byte[] v) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
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
                lock.unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (Exception e) {
            checkTxRollBack();
            throw e;
        }
    }


    public byte[] get(String key, long i) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);

            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return null;
            }
            ValueK valueK = new ValueK(key_b.length, key_b, metaV.getVersion(), i);
            return getDB(valueK.convertValueBytes().toBytes(), SstColumnFamily.DEFAULT);
        }
    }

    public List<byte[]> get(String key, List<Long> is) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
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
    }

    public Long left(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return null;
            }
            return metaV.left;
        }
    }

    public Long right(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return null;
            }
            return metaV.right;
        }
    }


    public int size(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                return 0;
            }
            return metaV.getSize();
        }
    }


    public void deleteFast(String key) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return;
                }
                deleteFast(key_b, metaV);
            } finally {
                lock.unlock(lockEntity);
            }
            checkTxCommit();
        } catch (Exception e) {
            checkTxRollBack();
            throw e;
        }
    }

    public void delete(String key) throws KitDBException {
        checkTxRange();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return;
                }
                start();
                deleteDB(key_b, SstColumnFamily.META);
                delete(key_b, metaV);
                commit();
            } finally {
                lock.unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (KitDBException e) {
            checkTxRollBack();
            throw e;
        }

    }

    public boolean isExist(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            byte[] k_v = getDB(key_b, SstColumnFamily.META);
            MetaV metaV = addCheck(k_v);

            return metaV != null;
        }
    }


    public List<byte[]> range(String key, long start, long end) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
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
    }

    @Override
    @SuppressWarnings("unchecked")
    public RIterator<RList> iterator(String key) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
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
    }

    public Entry getEntry(RocksIterator iterator) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            byte[] key_bs = iterator.key();
            if (key_bs == null) {
                return null;
            }
            ValueK key = ValueKD.build(key_bs).convertValue();
            Entry entry = new Entry(key.index, iterator.value());
            return entry;
        }
    }


    public List<byte[]> blpop(String key, int num) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);

                List<byte[]> list = new ArrayList<>();
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
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
                    checkTxCommit();
                    return list;
                } catch (Exception e) {
                    throw e;
                } finally {
                    release();
                }
            } finally {
                lock.unlock(lockEntity);
            }

        } catch (Exception e) {
            checkTxRollBack();
            throw e;
        }
    }


    public List<byte[]> brpop(String key, int num) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);
                List<byte[]> list = new ArrayList<>();
                MetaV metaV = getMeta(key_b);
                if (metaV == null) {
                    checkTxCommit();
                    return list;
                }
                try (final RocksIterator iterator = newIterator(SstColumnFamily.DEFAULT)) {
                    final int maxCount = num > 0 ? num : Integer.MAX_VALUE;
                    ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.right);
                    List<byte[]> delete_keys = new ArrayList<>();
                    ValueKD valueKD = valueK_seek.convertValueBytes();
                    byte[] heads = valueKD.toHeadBytes();
                    iterator.seekForPrev(valueKD.toBytes());
                    int count = 0;
                    while (iterator.isValid() && count++ < maxCount) {
                        byte[] ikey = iterator.key();
                        if (!BytesUtil.checkHead(heads, ikey)) break;
                        delete_keys.add(ikey);
                        list.add(iterator.value());
                        iterator.prev();
                    }

                    metaV.setSize(metaV.getSize() - delete_keys.size());
                    if (metaV.getSize() != 0) {
                        byte[] ikey = iterator.key();
                        ValueKD key_bytes = ValueKD.build(ikey);
                        metaV.setRight(key_bytes.getIndexV());
                    }
                    start();
                    putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                    for (byte[] delete_key : delete_keys) {
                        deleteDB(delete_key, SstColumnFamily.DEFAULT);
                    }
                    commit();
                    checkTxCommit();
                    return list;
                } finally {
                    release();
                }

            } finally {
                lock.unlock(lockEntity);
            }
        } catch (Exception e) {
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

    public void ttl(String key, int ttl) throws KitDBException {
        DAssert.isTrue(ttl > 0, ErrorType.PARAM_ERROR, "ttl must greater than 0");

        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);
                start();
                MetaV metaV = getMeta(key_b);

                DAssert.notNull(metaV, ErrorType.NOT_EXIST, "The List does not exist.");
                delTimerCollection(KeyEnum.COLLECT_TIMER,
                        metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());

                metaV.setTimestamp((int) (System.currentTimeMillis() / 1000 + ttl));

                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                setTimerCollection(KeyEnum.COLLECT_TIMER,
                        metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                commit();

            } finally {
                lock.unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (Exception e) {
            checkTxRollBack();
            throw e;
        }
    }

    public void delTtl(String key) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            byte[] key_b = getKey(key);
            MetaV metaV = getMeta(key_b);
            if (metaV == null) {
                checkTxCommit();
                return;
            }
            LockEntity lockEntity = lock.lock(key);
            try {
                start();
                delTimerCollection(KeyEnum.COLLECT_TIMER,
                        metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                metaV.setTimestamp(-1);
                putDB(key_b, metaV.convertMetaBytes().toBytes(), SstColumnFamily.META);
                commit();
            } finally {
                lock.unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (Exception e) {
            checkTxRollBack();
            throw e;
        }
    }

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


    public void addMayTTL(String key, byte[] v, int ttl) throws KitDBException {
        DAssert.isTrue(ttl > 0, ErrorType.PARAM_ERROR, "ttl must greater than 0");
        addMayTTLPrivate(key, v, ttl);
    }


    private void delete(byte[] key_b, MetaV metaV) {
        ValueK valueK_seek = new ValueK(key_b.length, key_b, metaV.getVersion(), metaV.left);
        MetaVD metaVD = metaV.convertMetaBytes();
        ValueKD valueKD = valueK_seek.convertValueBytes();
        byte[] heads = valueKD.toHeadBytes();
        deleteHead(heads, SstColumnFamily.DEFAULT);
        byte[] key = ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion());
        deleteDB(key, SstColumnFamily.DEFAULT);
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


    protected byte[] getKey(String key) throws KitDBException {
        DAssert.notNull(key, ErrorType.NULL, "Key is null");
        return ArrayKits.addAll(HEAD_B, key.getBytes(charset));
    }

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

    private MetaV getMetaP(byte[] key_b) throws KitDBException {
        byte[] k_v = getDB(key_b, SstColumnFamily.META);
        if (k_v == null) return null;

        MetaV metaV = MetaVD.build(k_v).convertMeta();
        return metaV;
    }


    protected synchronized void deleteByClear(byte[] key_b, MetaV metaV) throws KitDBException {
        try (CloseLock ignored = checkClose()) {
            start();
            delete(key_b, metaV);
            commitLocal();
        } finally {
            release();
        }
    }


    protected void deleteTTL(int time, byte[] key_b, byte[] meta_b) throws KitDBException {
        String key = new String(ArrayUtil.sub(key_b, 1, key_b.length + 1), charset);
        LockEntity lockEntity = lock.lock(key);
        try (CloseLock ignored = checkClose()) {
            MetaV metaV = getMetaP(key_b);
            if (time != metaV.timestamp) {
                return;
            }
            deleteTTL(key_b, MetaVD.build(meta_b).convertMeta(), metaV.version);
        } finally {
            lock.unlock(lockEntity);
        }
    }


    private void addAllMayTTLPrivate(String key, List<byte[]> vs, int ttl) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);
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

                    if (ttl != -1) {
                        setTimerCollection(KeyEnum.COLLECT_TIMER,
                                metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                    }
                }
                commit();
            } finally {
                lock.unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (Exception e) {
            log.error("error", e);
            checkTxRollBack();
            throw e;
        }
    }


    /**
     * 如果新建则设置设置TTL。如果已存在则不设置
     *
     * @param v
     * @param ttl
     * @throws RocksDBException
     */
    private void addMayTTLPrivate(String key, byte[] v, int ttl) throws KitDBException {
        checkTxStart();
        try (CloseLock ignored = checkClose()) {
            LockEntity lockEntity = lock.lock(key);
            try {
                byte[] key_b = getKey(key);
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
                    if (ttl != -1) {
                        setTimerCollection(KeyEnum.COLLECT_TIMER,
                                metaV.getTimestamp(), key_b, metaV.convertMetaBytes().toBytesHead());
                    }
                }
                commit();
            } finally {
                lock.unlock(lockEntity);
                release();
            }
            checkTxCommit();
        } catch (Exception e) {
            checkTxRollBack();
            throw e;
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

        public byte[] toBytesHead() {
            byte[] value = ArrayKits.addAll(HEAD_B, ArrayKits.intToBytes(0),
                    ArrayKits.longToBytes(0),
                    ArrayKits.longToBytes(0),
                    ArrayKits.intToBytes(0), this.version);
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
