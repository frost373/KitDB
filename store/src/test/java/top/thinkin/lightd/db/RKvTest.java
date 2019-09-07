package top.thinkin.lightd.db;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.benchmark.JoinFuture;
import top.thinkin.lightd.kit.ArrayKits;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RKvTest {

    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors * 8);


    static DB db;

    @Before
    public void init() throws RocksDBException {
        if (db == null) {
            RocksDB.loadLibrary();
            db = DB.build("D:\\temp\\db", true);
        }
    }

    /**
     * 多线程下的单插入
     *
     * @throws Exception
     */
    @Test
    public void set() throws Exception {
        String head = "setA";

        RKv kv = db.getrKv();
        try {
            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (int i = 0; i < 100 * 10000; i++) {
                int fj = i;
                joinFuture.add(args -> {
                    try {
                        kv.set(head + fj, ("test" + fj).getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();

            for (int i = 0; i < 100 * 10000; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }
        } finally {
            kv.delPrefix(head);
        }

    }

    /**
     * 多线程下的自增
     *
     * @throws Exception
     */
    @Test
    public void incr() throws Exception {
        String head = "incrA";
        RKv kv = db.getrKv();
        try {
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
            Assert.assertArrayEquals(ArrayKits.longToBytes(100 * 10000), kv.get(head));
        } finally {
            kv.delPrefix(head);
        }
    }

    /**
     * 多线程下的批量PUT
     */
    @Test
    public void set1() throws Exception {
        String head = "setB";
        RKv kv = db.getrKv();
        try {
            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (int i1 = 0; i1 < 100; i1++) {
                int finalI = i1;
                joinFuture.add(args -> {
                    try {
                        List<RKv.Entry> entries = new ArrayList<>(10000);

                        for (int j = 0; j < 10000; j++) {
                            entries.add(new RKv.Entry(
                                    head + (finalI * 10000 + j),
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
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(("test" + i).getBytes(), bytes);
            }
        } finally {
            kv.delPrefix(head);
        }

    }

    /**
     * 多线程下的批量PUT和生存时间
     * @throws Exception
     */
    @Test
    public void set2() throws Exception {
        String head = "setC";
        RKv kv = db.getrKv();

        int num = 1000;

        try {
            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (int i1 = 0; i1 < 100; i1++) {
                int finalI = i1;
                joinFuture.add(args -> {
                    try {
                        List<RKv.Entry> entries = new ArrayList<>(num);

                        for (int j = 0; j < num; j++) {
                            entries.add(new RKv.Entry(
                                    head + (finalI * num + j),
                                    ("test" + (finalI * num + j)).getBytes()));
                        }
                        kv.set(entries, 3);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();

            for (int i = 0; i < 100 * num; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(("test" + i).getBytes(), bytes);
            }

            Thread.sleep(3 * 1000);

            for (int i = 0; i < 100 * num; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertNull(bytes);
            }
        } finally {
            kv.delPrefix(head);
        }

    }

    /**
     * PUT的同时+生存时间
     * @throws Exception
     */
    @Test
    public void setTTL() throws Exception {
        RKv kv = db.getrKv();
        String head = "setTTLA";
        try {
            for (int i = 0; i < 100; i++) {
                kv.set(head + i, ("test" + i).getBytes(), 3);
            }

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }

            Thread.sleep(3 * 1000);

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertNull(bytes);
            }
        } finally {
            kv.delPrefix(head);
        }


    }

    @Test
    public void ttlandPut() throws Exception {
        RKv kv = db.getrKv();
        String head = "ttlandPut";
        try {
            for (int i = 0; i < 100; i++) {
                kv.set(head + i, ("test" + i).getBytes(), 1);
            }

            for (int i = 0; i < 100; i++) {
                kv.set(head + i, ("test" + i).getBytes());
            }
            Thread.sleep(2 * 1000);
            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }
        } catch (Exception e) {
            kv.delPrefix(head);
        }
    }

    /**
     * 单独设置生存时间
     */
    @Test
    public void ttl() throws Exception {
        RKv kv = db.getrKv();
        String head = "ttlA";
        try {
            for (int i = 0; i < 100; i++) {
                kv.set(head + i, ("test" + i).getBytes());
            }

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }

            for (int i = 0; i < 100; i++) {
                kv.ttl(head + i, 1);
            }

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }

            for (int i = 0; i < 100; i++) {
                kv.ttl(head + i, 4);
            }

            Thread.sleep(3 * 1000);

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }

            Thread.sleep(1 * 1000);

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertNull(bytes);
            }
        } finally {
            kv.delPrefix(head);
        }

    }

    /**
     * 单个GET
     */
    @Test
    public void get() throws Exception {
        RKv kv = db.getrKv();
        String head = "getA";


        kv.delPrefix(head);

    }

    /**
     * 批量GET
     */
    @Test
    public void get1() throws Exception {
        RKv kv = db.getrKv();
        String head = "getB";
        try {
            String[] strings = new String[100];

            for (int i = 0; i < 100; i++) {
                kv.set(head + i, ("test" + i).getBytes());
                strings[i] = head + i;
            }
            Map<String, byte[]> map = kv.get(strings);

            for (int i = 0; i < 100; i++) {
                byte[] bytes = map.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }

            for (int i = 0; i < 100; i++) {
                kv.ttl(head + i, 3);
            }
            for (int i = 0; i < 100; i++) {
                byte[] bytes = map.get(head + i);
                Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
            }

            Thread.sleep(3 * 1000);

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertNull(bytes);
            }
        } finally {
            kv.delPrefix(head);

        }

    }

    /**
     * 非严格校验TTL
     */
    @Test
    public void getNoTTL() throws Exception {
        RKv kv = db.getrKv();
        String head = "getNoTTL";

        for (int i = 0; i < 1; i++) {
            kv.set(head + i, ("test" + i).getBytes(), 1);
        }

        for (int i = 0; i < 1; i++) {
            byte[] bytes = kv.getNoTTL(head + i);
            Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
        }

        Thread.sleep(5 * 1000);

        for (int i = 0; i < 1; i++) {
            byte[] bytes = kv.getNoTTL(head + i);
            Assert.assertNull(bytes);
        }
    }

    @Test
    public void del() throws Exception {
        RKv kv = db.getrKv();
        String head = "delA";

        try {

            for (int i = 0; i < 10000; i++) {
                kv.set(head + i, ("test" + i).getBytes());
            }

            for (int i = 0; i < 10000; i++) {
                kv.del(head + i);
            }

            for (int i = 0; i < 10000; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertNull(bytes);
            }
        } finally {
            kv.delPrefix(head);
        }

    }


    @Test
    public void delPrefix() throws Exception {
        RKv kv = db.getrKv();
        String head = "delPrefixA";

        try {
            for (int i = 0; i < 1000 * 10000; i++) {
                kv.set(head + i, ("test" + i).getBytes());
            }
            kv.delPrefix(head);
            for (int i = 0; i < 1000 * 10000; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertNull(bytes);
            }
        } finally {
            kv.delPrefix(head);
        }
    }

    @Test
    public void keys() throws Exception {
        RKv kv = db.getrKv();
        String head = "keysA";
        try {
            Set<String> keys1 = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                kv.set(head + i, ("test" + i).getBytes());
                keys1.add(head + i);
            }
            List<String> keys = kv.keys("keysA", 0, 999999);

            Set<String> sets = new TreeSet<>(keys);

            Assert.assertTrue(sets.containsAll(keys1));
            Assert.assertTrue(keys1.containsAll(sets));
            sets.clear();

            for (int i = 0; i < 10; i++) {
                List<String> keys_i = kv.keys("keysA", (i * 10), 10);

                sets.addAll(keys_i);
                Assert.assertEquals(10, keys_i.size());
            }

            Assert.assertEquals(100, sets.size());

            Assert.assertTrue(sets.containsAll(keys1));
            Assert.assertTrue(keys1.containsAll(sets));
        } finally {
            kv.delPrefix(head);
        }
    }

    @Test
    public void getTtl() throws Exception {
       /* RKv kv = db.getrKv();
        String head = "delTtlA";

        for (int i = 0; i < 199999999; i++) {
            kv.set(head+i, ("test" + i).getBytes(),3);
        }*/


    }

    @Test
    public void delTtl() throws Exception {
        RKv kv = db.getrKv();
        String head = "delTtlA";

        try {
            for (int i = 0; i < 10000; i++) {
                kv.set(head + i, ("test" + i).getBytes(), 3);
            }

            for (int i = 0; i < 10000; i++) {
                kv.delTtl(head + i);
            }

            Thread.sleep(3 * 1000);

            for (int i = 0; i < 100; i++) {
                byte[] bytes = kv.get(head + i);
                Assert.assertNotNull(bytes);
            }
        } catch (Exception e) {
            kv.delPrefix(head);
        }
    }
}