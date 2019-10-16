package top.thinkin.lightd.db;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.benchmark.JoinFuture;
import top.thinkin.lightd.exception.KitDBException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class RSetTest {
    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors * 8);
    static DB db;

    @Before
    public void init() throws RocksDBException {
        if (db == null) {
            try {
                db = DB.build("D:\\temp\\db", true);
            } catch (Exception e) {
                log.error("error", e);
                e.printStackTrace();
            }
        }
    }


    @AfterClass
    public static void after() throws InterruptedException {
        Thread.sleep(5000);
    }


    @Test
    public void add() throws KitDBException {
        String head = "add0";
        RSet set = db.getSet();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                set.add(head, ("hello world" + i).getBytes());
            }

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(set.contains(head, ("hello world" + i).getBytes()));
            }

            Assert.assertEquals(num, set.size(head));
        } finally {
            set.delete(head);
        }


        try {
            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (int i = 0; i < num; i++) {
                int fj = i;
                joinFuture.add(args -> {
                    try {
                        set.add(head, ("hello world" + fj).getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(set.contains(head, ("hello world" + i).getBytes()));
            }
            Assert.assertEquals(num, set.size(head));

        } finally {
            set.delete(head);
        }

    }


    @Test
    public void pop() throws KitDBException {
        String head = "pop0";
        RSet set = db.getSet();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                set.add(head, ("hello world" + i).getBytes());
            }
            Set<String> strings = new HashSet<>();
            int n = num / 1000;
            for (int i = 0; i < n; i++) {
                List<byte[]> list = set.pop(head, 1000);
                Assert.assertEquals(num - 1000 * (i + 1), set.size(head));
                for (byte[] bytes : list) {
                    strings.add(new String(bytes));
                }
            }
            Assert.assertEquals(num, strings.size());
            for (int i = 0; i < num; i++) {
                Assert.assertTrue(strings.contains("hello world" + i));
            }
        } finally {
            set.delete(head);
        }
    }

    @Test
    public void remove() throws KitDBException {
        String head = "remove0";
        RSet set = db.getSet();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                set.add(head, ("hello world" + i).getBytes());
            }

            for (int i = 0; i < num; i++) {
                set.remove(head, ("hello world" + i).getBytes());
                Assert.assertEquals(num - (i + 1), set.size(head));
            }

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(!set.contains(head, ("hello world" + i).getBytes()));
            }

        } finally {
            set.delete(head);
        }
    }

    @Test
    public void iterator() throws KitDBException {
        String head = "iterator0";
        RSet set = db.getSet();
        int num = 10 * 10000;
        try {
            Set<String> stringSet = new HashSet<>();

            for (int i = 0; i < num; i++) {
                set.add(head, ("hello world" + i).getBytes());
                stringSet.add(("hello world" + i));

            }
            List<String> stringList = new ArrayList<>();
            try (RIterator<RSet> iterator = set.iterator(head)) {
                while (iterator.hasNext()) {
                    RSet.Entry er = iterator.next();
                    stringList.add(new String(er.getValue()));
                }
            }
            Assert.assertEquals(num, stringList.size());
            Assert.assertTrue(stringSet.containsAll(stringList));
            Assert.assertTrue(stringList.containsAll(stringSet));
        } finally {
            set.delete(head);
        }

    }

    @Test
    public void addMayTTL() throws KitDBException, InterruptedException {
        String head = "addMayTTL0";
        RSet set = db.getSet();
        int num = 10 * 10000;

        for (int i = 0; i < num; i++) {
            set.addMayTTL(head, 5, ("hello world" + i).getBytes());
        }
        Thread.sleep(5000);
        Assert.assertTrue(!set.isExist(head));

        for (int i = 0; i < num; i++) {
            Assert.assertTrue(!set.contains(head, ("hello world" + i).getBytes()));
        }


    }


    @Test
    public void getKeyIterator() {

    }


    @Test
    public void deleteFast() {

    }


    @Test
    public void getTtl() {

    }

    @Test
    public void delTtl() {

    }

    @Test
    public void ttl() {

    }


    @Test
    public void isExist() {
    }

    @Test
    public void size() {
    }

    @Test
    public void getEntry() {
    }

    @Test
    public void delete() {

    }

    @Test
    public void isMember() {
    }
}