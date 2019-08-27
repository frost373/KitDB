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
    private ZSet ttlZset;

    private RKv rKv;
    private final static byte[] DEL_HEAD = "D".getBytes();
    private WriteOptions writeOptions;

    private RocksDB binLogDB;
    private BinLog binLog;
    static ScheduledThreadPoolExecutor stp = new ScheduledThreadPoolExecutor(3);

    public final ConcurrentHashMap map = new ConcurrentHashMap();

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

    public ZSet ttlZset() {
        return this.ttlZset;
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


    public synchronized void checkTTL() {
        try {
            List<ZSet.Entry> outTimeKeys = ttlZset.range(System.currentTimeMillis() / 1000, Integer.MAX_VALUE);
            for (ZSet.Entry outTimeKey : outTimeKeys) {
                byte[] key_bs = outTimeKey.getValue();
                if (RList.HEAD_B[0] == key_bs[0]) {
                    RList.deleteFast(key_bs, this);
                }

                if (ZSet.HEAD_B[0] == key_bs[0]) {
                    ZSet.deleteFast(key_bs, this);
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
        db.ttlZset = db.getZSet(ReservedWords.ZSET_KEYS.TTL);

        db.rKv = new RKv(db);

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
        db.ttlZset = db.getZSet(ReservedWords.ZSET_KEYS.TTL);

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


    public synchronized RList getList(String key) {
        Object list = map.get(RList.HEAD + key);
        if (list == null) {
            list = new RList(this, key);
            map.put(RList.HEAD + key, list);
        }
        return (RList) list;
    }

    public synchronized Sequence getSequence(String key) {
        Object s = map.get(Sequence.HEAD + key);
        if (s == null) {
            s = new Sequence(this, key.getBytes());
            map.put(Sequence.HEAD + key, s);
        }
        return (Sequence) s;
    }


    public synchronized ZSet getZSet(String key) {
        Object zset = map.get(ZSet.HEAD + key);
        if (zset == null) {
            zset = new ZSet(this, key);
            map.put(ZSet.HEAD + key, zset);
        }
        return (ZSet) zset;
    }


    public synchronized RSet getSet(String key) {
        Object list = map.get(RList.HEAD + key);
        if (list == null) {
            list = new RSet(this, key);
            map.put(RSet.HEAD + key, list);
        }
        return (RSet) list;
    }


    public synchronized RMap getRMap(String key) {
        Object rmap = map.get(RMap.HEAD + key);
        if (rmap == null) {
            rmap = new RMap(this, key);
            map.put(RMap.HEAD + key, rmap);
        }
        return (RMap) rmap;
    }

    public RKv getrKv() {
        return rKv;
    }

    public BinLog getBinLog() {
        return binLog;
    }
}
