package top.thinkin.lightd.db;

import lombok.extern.log4j.Log4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.benchmark.JoinFuture;
import top.thinkin.lightd.kit.ArrayKits;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Log4j
public class RKvTest {

    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors * 8);


    static DB db;

    @Before
    public void init() throws RocksDBException {
        if (db == null) {
            RocksDB.loadLibrary();
            db = DB.build("D:\\temp\\db", false);
        }
    }


    //@Test
    public void set() throws Exception {
        byte[] head = "set".getBytes();

        RKv kv = db.getrKv();
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int i = 0; i < 100 * 10000; i++) {
            int fj = i;
            joinFuture.add(args -> {
                try {
                    kv.set(ArrayKits.intToBytes(fj), ("test" + fj).getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return "";
            });
        }
        joinFuture.join();

        for (int i = 0; i < 100 * 10000; i++) {
            byte[] bytes = kv.get(ArrayKits.intToBytes(i));
            Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
        }

    }

    //@Test
    public void incr() throws Exception {
        byte[] head = "incr".getBytes();

        RKv kv = db.getrKv();
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int i = 0; i < 100 * 10000; i++) {
            int fj = i;
            joinFuture.add(args -> {
                try {
                    kv.incr(head, 1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return "";
            });
        }
        joinFuture.join();
        kv.del(head);
    }

    @Test
    public void set1() throws RocksDBException {
        byte[] head = "set1".getBytes();
        RKv kv = db.getrKv();

        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int i1 = 0; i1 < 100; i1++) {
            int finalI = i1;
            joinFuture.add(args -> {
                try {
                    List<RKv.Entry> entries = new ArrayList<>(10000);

                    for (int j = 0; j < 10000; j++) {
                        entries.add(new RKv.Entry(
                                ArrayKits.addAll(head, ArrayKits.intToBytes(finalI * 10000 + j)),
                                ("test" + (finalI * 10000 + j)).getBytes()));
                    }
                    kv.set(entries);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return "";
            });
        }
        joinFuture.join();

        for (int i = 0; i < 100 * 10000; i++) {
            byte[] bytes = kv.get(ArrayKits.addAll(head, ArrayKits.intToBytes(i)));
            Assert.assertArrayEquals(("test" + i).getBytes(), bytes);
        }

    }


    @Test
    public void set2() {

    }

    // @Test
    public void setTTL() throws Exception {
        RKv kv = db.getrKv();
        byte[] head = "setTTL".getBytes();
        for (int i = 0; i < 100; i++) {
            kv.setTTL(ArrayKits.addAll(head, ArrayKits.intToBytes(i)), ("test" + i).getBytes(), 200);
        }

        for (int i = 0; i < 100; i++) {
            byte[] bytes = kv.get(ArrayKits.addAll(head, ArrayKits.intToBytes(i)));
            Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
        }

        Thread.sleep(20 * 1000);
        for (int i = 0; i < 100; i++) {
            byte[] bytes = kv.get(ArrayKits.addAll(head, ArrayKits.intToBytes(i)));
            Assert.assertNull(bytes);
        }


    }

    @Test
    public void ttl() {
    }

    @Test
    public void get() {
    }

    @Test
    public void get1() {
    }

    @Test
    public void getNoTTL() {
    }

    @Test
    public void del() {
    }

    @Test
    public void release() {
    }

    @Test
    public void delPrefix() {
    }

    @Test
    public void keys() {
    }

    @Test
    public void getTtl() {
    }

    @Test
    public void delTtl() {
    }
}