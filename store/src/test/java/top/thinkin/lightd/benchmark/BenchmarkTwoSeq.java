package top.thinkin.lightd.benchmark;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.RKv;
import top.thinkin.lightd.db.Sequence;
import top.thinkin.lightd.exception.KitDBException;

public class BenchmarkTwoSeq {

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        DB db = DB.build("C:\\tmp\\dbx", false);
        //seq(db);
        kvSeq(db);
        db.close();
    }

    private static void seq(DB db) throws KitDBException, RocksDBException {
        long start = System.currentTimeMillis(); //获取开始时间
        Sequence sequence = db.getSequence("hello");
        for (int i = 0; i < 1000 * 10000; i++) {
            sequence.incr(1L);
        }
        long end = System.currentTimeMillis(); //获取结束时间
        System.out.println("start time:" + start + "; end time:" + end + "; Run Time:" + (end - start) + "(ms)");
    }


    private static void kvSeq(DB db) throws KitDBException, RocksDBException {
        long start = System.currentTimeMillis(); //获取开始时间
        RKv kv = db.getrKv();
        for (int i = 0; i < 1000 * 10000; i++) {
            kv.incr("test", 1);
        }
        long end = System.currentTimeMillis(); //获取结束时间
        System.out.println("start time:" + start + "; end time:" + end + "; Run Time:" + (end - start) + "(ms)");
    }


}
