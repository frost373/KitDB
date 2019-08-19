package top.thinkin.lightd.benchmark;

import org.rocksdb.RocksDB;
import top.thinkin.lightd.collect.DB;
import top.thinkin.lightd.collect.Sequence;

public class BVS {
    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        DB db = DB.build("D:\\temp\\db");
        Sequence sequence = db.getSequence("test");
        try {
            long startTime = System.currentTimeMillis(); //获取开始时间
            for (int i = 0; i < 100 * (10000); i++) {
                sequence.incr(1L);
            }
            System.out.println("version" + sequence.get()); //输出程序运行时间
            long endTime = System.currentTimeMillis(); //获取结束时间
            System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间
            System.out.println("benchmark" + ((10.00 * 10000) / (endTime - startTime)) * 1000 + "per second"); //输出程序运行时间


        } finally {
            db.close();
        }
    }
}
