package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import top.thinkin.lightd.base.BinLog;
import top.thinkin.lightd.base.KeySegmentLockManager;
import top.thinkin.lightd.base.VersionSequence;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.kit.BytesUtil;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DB extends DBAbs {
    static final byte[] DB_VERSION = "V0.0.2".getBytes();


    protected static Charset charset = Charset.forName("UTF-8");

    private VersionSequence versionSequence;
    private ZSet zSet;
    private RMap map;
    private RSet set;
    private RList list;

    private RKv rKv;
    private final static byte[] DEL_HEAD = "D".getBytes();

    private RocksDB binLogDB;

    private BinLog binLog;

    ScheduledThreadPoolExecutor stp = new ScheduledThreadPoolExecutor(4);


    public final ConcurrentHashMap map1 = new ConcurrentHashMap();

    static {
        RocksDB.loadLibrary();
    }


    private DB() {
        super();
    }

    public void close() {
        if (stp != null) {
            stp.shutdown();
        }
        if (rocksDB != null) {
            rocksDB.close();
        }

        if (binLogDB != null) {
            binLogDB.close();
        }
    }

    public RSnapshot createSnapshot() {
        return new RSnapshot(this.rocksDB.getSnapshot());
    }

    public VersionSequence versionSequence() {
        return this.versionSequence;
    }

    public synchronized void clear() {
        try (RocksIterator iterator = this.rocksDB.newIterator()) {
            iterator.seek(DEL_HEAD);
            while (iterator.isValid()) {
                byte[] key_bs = iterator.key();
                if (DEL_HEAD[0] != key_bs[0]) {
                    break;
                }
                byte[] value = iterator.value();
                iterator.next();
                byte[] rel_key_bs = ArrayUtil.sub(key_bs, 1, key_bs.length - 4);
                if (RList.HEAD_B[0] == rel_key_bs[0]) {
                    RList.MetaV metaV = RList.MetaVD.build(value).convertMeta();
                    this.list.deleteByClear(rel_key_bs, metaV);
                }
                if (ZSet.HEAD_B[0] == rel_key_bs[0]) {
                    //ZSet.delete(rel_key_bs, value, this);
                }

            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public ZSet getzSet2() {
        return zSet;
    }

    public RMap getMap() {
        return map;
    }

    public RSet getSet() {
        return set;
    }

    public RList getList() {
        return list;
    }

    public synchronized void checkTTL() {
        try {
            int end = (int) (System.currentTimeMillis() / 1000);
            for (int i = 0; i < 10; i++) {
                TimerStore.rangeDel(this,
                        KeyEnum.COLLECT_TIMER.getKey(), 0, end, 500, dataList -> {
                            List<TimerStore.TData> outTimeKeys = dataList;
                            for (TimerStore.TData outTimeKey : outTimeKeys) {
                                byte[] value = outTimeKey.getValue();
                                RBase.TimerCollection timerCollection = RBase.getTimerCollection(value);
                                if (RList.HEAD_B[0] == timerCollection.meta_b[0]) {
                                    this.list.deleteFast(timerCollection.key_b, timerCollection.meta_b);
                                }
                            }
                        });
            }
        } catch (Exception e) {
            log.error("clearKV error", e);
        }
    }


    /**
     * 检测泄露的KV——TTL
     * TODO
     */
    public synchronized void checkKVTTL() {

    }

    public synchronized void clearKV() {
        try {
            int end = (int) (System.currentTimeMillis() / 1000);
            for (int i = 0; i < 10; i++) {

                List<TimerStore.TData> outTimeKeys = TimerStore.rangeDel(this,
                        KeyEnum.KV_TIMER.getKey(), 0, end, 2000);
                if (outTimeKeys.size() == 0) {
                    return;
                }
                for (TimerStore.TData outTimeKey : outTimeKeys) {
                    byte[] key_bs = outTimeKey.getValue();
                    if (RKv.HEAD_B[0] == key_bs[0]) {
                        this.rKv.delCheckTTL(
                                new String(ArrayUtil.sub(key_bs, 1, key_bs.length + 1), charset),
                                outTimeKey.getTime());
                    }
                }
            }
        } catch (Exception e) {
            log.error("clearKV error", e);
        }
    }


    public synchronized void writeSnapshot() throws RocksDBException {
        try (final Checkpoint checkpoint = Checkpoint.create(this.rocksDB)) {
            final File tempFile = new File("D:\\temp\\sp");
            if (tempFile.exists()) {
                tempFile.delete();
            }
            checkpoint.createCheckpoint("D:\\temp\\sp");
        } finally {

        }
    }


    public synchronized static DB build(String dir) throws RocksDBException {
        return build(dir, true);
    }


    public synchronized static DB build(String dir, boolean autoclear) throws RocksDBException {
        DB db = new DB();
        DBOptions options = getDbOptions();

        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        db.rocksDB = RocksDB.open(options, dir, getColumnFamilyDescriptor(), cfHandles);

        setDB(autoclear, db, cfHandles);

        return db;
    }

    public synchronized static DB buildTransactionDB(String dir, boolean autoclear) throws RocksDBException {
        DB db = new DB();
        DBOptions options = getDbOptions();
        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
        TransactionDB rocksDB = TransactionDB.open(options, transactionDBOptions, dir, getColumnFamilyDescriptor(), cfHandles);
        db.openTransaction = true;

        db.rocksDB = rocksDB;
        setDB(autoclear, db, cfHandles);

        return db;
    }

    private static DBOptions getDbOptions() {
        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        return options;
    }

    private static void setDB(boolean autoclear, DB db, List<ColumnFamilyHandle> cfHandles) throws RocksDBException {
        db.metaHandle = cfHandles.get(0);
        db.defHandle = cfHandles.get(1);

        db.versionSequence = new VersionSequence(db.rocksDB);
        byte[] version = db.rocksDB.get("version".getBytes());
        if (version == null) {
            db.rocksDB.put("version".getBytes(), DB_VERSION);
        } else {
            DAssert.isTrue(BytesUtil.compare(version, DB_VERSION) == 0, ErrorType.STORE_VERSION,
                    "Store versions must be " + new String(DB_VERSION) + ", but now is " + new String(version));
        }


        db.writeOptions = new WriteOptions();
        if (autoclear) {
            db.stp.scheduleWithFixedDelay(db::clear, 2, 2, TimeUnit.SECONDS);
            db.stp.scheduleWithFixedDelay(db::clearKV, 1, 1, TimeUnit.SECONDS);
        }
        db.stp.scheduleWithFixedDelay(db::checkTTL, 1, 1, TimeUnit.SECONDS);

        Options optionsBinLog = new Options();
        optionsBinLog.setCreateIfMissing(true);

        db.binLogDB = RocksDB.open(optionsBinLog, "D:\\temp\\logs");
        db.binLog = new BinLog(db.binLogDB);
        db.keySegmentLockManager = new KeySegmentLockManager(db.stp);
        db.tx_lock = db.keySegmentLockManager.createLock(2000);
        db.rKv = new RKv(db);
        db.zSet = new ZSet(db);
        db.set = new RSet(db);
        db.list = new RList(db);
        db.map = new RMap(db);
    }


    public RKv getrKv() {
        return rKv;
    }

    public BinLog getBinLog() {
        return binLog;
    }


    public KeySegmentLockManager getKeySegmentLockManager() {
        return keySegmentLockManager;
    }
}
