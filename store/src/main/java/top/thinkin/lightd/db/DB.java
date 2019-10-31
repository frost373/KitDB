package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import top.thinkin.lightd.base.BinLog;
import top.thinkin.lightd.base.CloseLock;
import top.thinkin.lightd.base.KeySegmentLockManager;
import top.thinkin.lightd.base.VersionSequence;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.KitDBException;
import top.thinkin.lightd.kit.BytesUtil;
import top.thinkin.lightd.kit.FileZipUtils;
import top.thinkin.lightd.kit.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipOutputStream;

@Slf4j
public class DB extends DBAbs {
    static final byte[] DB_VERSION = "V0.0.2".getBytes();

    public static String BACK_FILE_SUFFIX = ".kit";

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

    static {
        RocksDB.loadLibrary();
    }


    private DB() {
        super();
    }

    public synchronized void close() throws InterruptedException, KitDBException {
        try (CloseLock ignored = closeCheck()) {
            open = false;
            if (stp != null) {
                stp.shutdown();
                stp.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            }
            if (rocksDB != null) {
                rocksDB.close();
                this.readOptions.close();
                this.writeOptions.close();
                this.options.close();
                for (final ColumnFamilyOptions cfOptions : this.cfOptionsList) {
                    cfOptions.close();
                }
                this.metaHandle.close();
                this.defHandle.close();
            }
        }
    }


    public synchronized void stop() throws InterruptedException, KitDBException {
        try (CloseLock ignored = closeCheck()) {
            open = false;
            if (stp != null) {
                stp.shutdown();
                stp.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            }
            if (rocksDB != null) {
                rocksDB.close();
                this.metaHandle.close();
                this.defHandle.close();
            }
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
            while (iterator.isValid() && open) {
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

                if (RMap.HEAD_B[0] == rel_key_bs[0]) {
                    RMap.Meta meta = RMap.MetaD.build(value).convertMeta();
                    this.map.deleteByClear(rel_key_bs, meta);
                }

                if (RSet.HEAD_B[0] == rel_key_bs[0]) {
                    RSet.MetaD meta = RSet.MetaD.build(value);
                    this.set.deleteByClear(rel_key_bs, meta);
                }

                if (ZSet.HEAD_B[0] == rel_key_bs[0]) {
                    ZSet.MetaD meta = ZSet.MetaD.build(value);
                    this.zSet.deleteByClear(rel_key_bs, meta);
                }

            }
        } catch (final Exception e) {
            // TODO
            e.printStackTrace();
        }
    }

    public ZSet getzSet() {
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
                            DAssert.isTrue(open, ErrorType.DB_CLOSE, "db is closed");
                            for (TimerStore.TData outTimeKey : outTimeKeys) {
                                DAssert.isTrue(open, ErrorType.DB_CLOSE, "db is closed");
                                byte[] value = outTimeKey.getValue();
                                RBase.TimerCollection timerCollection = RBase.getTimerCollection(value);
                                if (RList.HEAD_B[0] == timerCollection.meta_b[0]) {
                                    this.list.deleteTTL(outTimeKey.getTime(),
                                            timerCollection.key_b, timerCollection.meta_b);
                                }

                                if (RMap.HEAD_B[0] == timerCollection.meta_b[0]) {
                                    this.map.deleteTTL(outTimeKey.getTime(),
                                            timerCollection.key_b, timerCollection.meta_b);
                                }

                                if (RSet.HEAD_B[0] == timerCollection.meta_b[0]) {
                                    this.set.deleteTTL(outTimeKey.getTime(),
                                            timerCollection.key_b, timerCollection.meta_b);
                                }

                                if (ZSet.HEAD_B[0] == timerCollection.meta_b[0]) {
                                    this.zSet.deleteTTL(outTimeKey.getTime(),
                                            timerCollection.key_b, timerCollection.meta_b);
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
                DAssert.isTrue(open, ErrorType.DB_CLOSE, "db is closed");
                List<TimerStore.TData> outTimeKeys = TimerStore.rangeDel(this,
                        KeyEnum.KV_TIMER.getKey(), 0, end, 2000);
                if (outTimeKeys.size() == 0) {
                    return;
                }
                for (TimerStore.TData outTimeKey : outTimeKeys) {
                    DAssert.isTrue(open, ErrorType.DB_CLOSE, "db is closed");
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


    public synchronized void compaction() {
        try {
            this.rocksDB.compactRange();
        } catch (Exception e) {
            log.error("compaction error", e);
        }
    }


    public synchronized String backupDB(String path, String backName) throws RocksDBException, IOException {
        Random r = new Random();
        String sourceDir = File.separator + "tempsp" + r.nextInt(999);
        String tempPath = path + sourceDir;
        final File tempFile = new File(tempPath);
        FileZipUtils.delFile(tempFile);
        try (final Checkpoint checkpoint = Checkpoint.create(this.rocksDB)) {
            checkpoint.createCheckpoint(tempPath);
        }
        String fileName = backName + BACK_FILE_SUFFIX;
        String backPath = path + File.separator + fileName;

        try (final FileOutputStream fOut = new FileOutputStream(backPath);
             final ZipOutputStream zOut = new ZipOutputStream(fOut)) {
            ZipUtil.compressDirectoryToZipFile(tempPath, "", zOut);
            fOut.getFD().sync();
        }

        FileZipUtils.delFile(tempFile);
        return backPath;
    }


    public static void releaseBackup(String path, String targetpath) throws IOException {
        ZipUtil.unzipFile(path, targetpath);
    }

    public synchronized static DB build(String dir) throws KitDBException {
        return build(dir, true);
    }


    public synchronized static DB build(String dir, boolean autoclear) throws KitDBException {
        DB db;
        try {
            db = new DB();
            DBOptions options = getDbOptions();
            db.options = options;
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            db.rocksDB = RocksDB.open(options, dir, db.getColumnFamilyDescriptor(), cfHandles);
            setDB(autoclear, db, cfHandles, false);
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }

        return db;
    }


    public synchronized static DB readOnly(String dir, boolean autoclear) throws KitDBException {
        DB db;
        try {
            db = new DB();
            DBOptions options = getDbOptions();
            db.options = options;
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            db.rocksDB = RocksDB.openReadOnly(options, dir, db.getColumnFamilyDescriptor(), cfHandles);
            setDB(autoclear, db, cfHandles, true);
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }

        return db;
    }

    public synchronized static DB buildTransactionDB(String dir, boolean autoclear) throws KitDBException {
        DB db;
        try {
            db = new DB();
            DBOptions options = getDbOptions();
            db.options = options;
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
            TransactionDB rocksDB = TransactionDB.open(options, transactionDBOptions, dir, db.getColumnFamilyDescriptor(), cfHandles);
            db.openTransaction = true;

            db.rocksDB = rocksDB;
            setDB(autoclear, db, cfHandles, false);
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
        return db;
    }

    private static DBOptions getDbOptions() {
        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        return options;
    }

    public synchronized void open(String dir, boolean autoclear, boolean readOnly) throws KitDBException {
        DAssert.isTrue(!open, ErrorType.DB_CLOSE, "db is closed");
        try {
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            this.rocksDB = RocksDB.open(options, dir, this.getColumnFamilyDescriptor(), cfHandles);
            this.metaHandle = cfHandles.get(0);
            this.defHandle = cfHandles.get(1);
            stp = new ScheduledThreadPoolExecutor(4);
            if (!readOnly) {
                if (autoclear) {
                    this.stp.scheduleWithFixedDelay(this::clear, 2, 2, TimeUnit.SECONDS);
                    this.stp.scheduleWithFixedDelay(this::clearKV, 1, 1, TimeUnit.SECONDS);
                }
                this.stp.scheduleWithFixedDelay(this::checkTTL, 1, 1, TimeUnit.SECONDS);
                this.stp.scheduleWithFixedDelay(this::compaction, 30, 30, TimeUnit.SECONDS);
            }
            this.keySegmentLockManager.start(stp);
            open = true;
        } catch (RocksDBException e) {
            throw new KitDBException(ErrorType.STROE_ERROR, e);
        }
    }



    private static void setDB(boolean autoclear, DB db, List<ColumnFamilyHandle> cfHandles, boolean readOnly) throws RocksDBException, KitDBException {
        db.metaHandle = cfHandles.get(0);
        db.defHandle = cfHandles.get(1);

        db.versionSequence = new VersionSequence(db);


        byte[] version = db.rocksDB.get("version".getBytes());
        if (version == null) {
            if (!readOnly) {
                db.rocksDB.put("version".getBytes(), DB_VERSION);
            } else {
                DAssert.isTrue(false, ErrorType.STORE_VERSION,
                        "Store versions must be " + new String(DB_VERSION) + ", but now is null");
            }
        } else {
            DAssert.isTrue(BytesUtil.compare(version, DB_VERSION) == 0, ErrorType.STORE_VERSION,
                    "Store versions must be " + new String(DB_VERSION) + ", but now is " + new String(version));
        }

        db.writeOptions = new WriteOptions();
        if (!readOnly) {
            if (autoclear) {
                db.stp.scheduleWithFixedDelay(db::clear, 2, 2, TimeUnit.SECONDS);
                db.stp.scheduleWithFixedDelay(db::clearKV, 1, 1, TimeUnit.SECONDS);
            }
            db.stp.scheduleWithFixedDelay(db::checkTTL, 1, 1, TimeUnit.SECONDS);
            db.stp.scheduleWithFixedDelay(db::compaction, 30, 30, TimeUnit.SECONDS);
        }

        Options optionsBinLog = new Options();
        optionsBinLog.setCreateIfMissing(true);

        db.binLogDB = null; //RocksDB.open(optionsBinLog, "D:\\temp\\logs2");
        //db.binLog = new BinLog(db.binLogDB);
        db.keySegmentLockManager = new KeySegmentLockManager(db.stp);
        db.rKv = new RKv(db);
        db.zSet = new ZSet(db);
        db.set = new RSet(db);
        db.list = new RList(db);
        db.map = new RMap(db);

        db.open = true;
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
