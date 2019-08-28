package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import org.rocksdb.*;
import top.thinkin.lightd.base.BinLog;
import top.thinkin.lightd.base.VersionSequence;
import top.thinkin.lightd.data.ReservedWords;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DB {


    private RocksDB rocksDB;
    private boolean openTransaction = false;

    public VersionSequence versionSequence;
    private ZSet zSet;
    private RMap map;
    private RSet set;
    private RList list;

    private RKv rKv;
    private final static byte[] DEL_HEAD = "D".getBytes();
    private WriteOptions writeOptions;

    private RocksDB binLogDB;
    private BinLog binLog;
    static ScheduledThreadPoolExecutor stp = new ScheduledThreadPoolExecutor(3);

    public final ConcurrentHashMap map1 = new ConcurrentHashMap();

    static {
        RocksDB.loadLibrary();
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

    public RocksDB rocksDB() {
        return this.rocksDB;
    }



    public VersionSequence versionSequence() {
        return this.versionSequence;
    }

    protected WriteOptions writeOptions() {
        return this.writeOptions;
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
                    RList.delete(rel_key_bs, value, this);
                }

                if (ZSet.HEAD_B[0] == rel_key_bs[0]) {
                    ZSet.delete(rel_key_bs, value, this);
                }

            }
        } catch (final Exception e) {
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
            List<ZSet.Entry> outTimeKeys = zSet.range(ReservedWords.ZSET_KEYS.TTL,
                    System.currentTimeMillis() / 1000, Integer.MAX_VALUE);
            for (ZSet.Entry outTimeKey : outTimeKeys) {
                byte[] key_bs = outTimeKey.getValue();
                if (RList.HEAD_B[0] == key_bs[0]) {
                    //RList.deleteFast(key_bs, this);
                }

                if (ZSet.HEAD_B[0] == key_bs[0]) {
                    //ZSet.deleteFast(key_bs, this);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
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

    public synchronized static DB buildTransactionDB(String dir) throws RocksDBException {
        DB db = new DB();
        Options options = new Options();
        options.setCreateIfMissing(true);

        TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
        TransactionDB rocksDB = TransactionDB.open(options, transactionDBOptions, dir);

        db.rocksDB = rocksDB;
        db.openTransaction = true;
        db.versionSequence = new VersionSequence(db.rocksDB);

        db.rKv = new RKv(db);
        db.zSet = new ZSet(db);
        db.set = new RSet(db);
        db.list = new RList(db);
        db.map = new RMap(db);


        db.writeOptions = new WriteOptions();
        stp.scheduleWithFixedDelay(db::clear, 2, 2, TimeUnit.SECONDS);
        //stp.scheduleWithFixedDelay(db::checkTTL, 2, 2, TimeUnit.SECONDS);

        return db;
    }

    public synchronized static DB build(String dir) throws RocksDBException {
        return build(dir, true);
    }

    public synchronized static DB build(String dir, boolean autoclear) throws RocksDBException {
        DB db = new DB();
        Options options = new Options();
        options.setCreateIfMissing(true);
        db.rocksDB = RocksDB.open(options, dir);
        db.versionSequence = new VersionSequence(db.rocksDB);

        db.rKv = new RKv(db);
        db.writeOptions = new WriteOptions();
        if (autoclear) {
            stp.scheduleWithFixedDelay(db::clear, 2, 2, TimeUnit.SECONDS);
        }
        db.binLogDB = RocksDB.open(options, "D:\\temp\\logs");

        db.binLog = new BinLog(db.binLogDB);


        //stp.scheduleWithFixedDelay(db::checkTTL, 2, 2, TimeUnit.SECONDS);
        return db;
    }








    public RKv getrKv() {
        return rKv;
    }

    public BinLog getBinLog() {
        return binLog;
    }
}
