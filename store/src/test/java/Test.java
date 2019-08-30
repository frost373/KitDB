import org.rocksdb.RocksDB;
import top.thinkin.lightd.base.SegmentLock;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.KeyIterator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    private static SegmentLock lock = new SegmentLock(32);
    static ExecutorService executorService = Executors.newFixedThreadPool(100);

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        DB db = DB.build("D:\\temp\\db", false);

        db.getList().add("hello", "i".getBytes());
        db.getList().add("hello2", "i".getBytes());
        db.getList().add("hello21", "i".getBytes());
        db.getList().add("some1", "i".getBytes());
        db.getList().delete("LPOP_LIST");
        try (KeyIterator keyIterator = db.getList().getKeyIterator()) {
            while (keyIterator.hasNext()) {
                String key = keyIterator.next();
                System.out.println(key);
            }
        }

    }
}
