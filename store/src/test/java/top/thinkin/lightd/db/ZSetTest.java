package top.thinkin.lightd.db;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.benchmark.FList;
import top.thinkin.lightd.benchmark.JoinFuture;
import top.thinkin.lightd.exception.KitDBException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ZSetTest {
    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors * 8);
    static DB db;

    @Before
    public void init() throws RocksDBException {
        if (db == null) {
            try {
                db = DB.build("D:\\temp\\db");
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
        ZSet set = db.getzSet();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                set.add(head, ("hello world" + i).getBytes(), i);
            }

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(set.contains(head, ("hello world" + i).getBytes()));
                long score = set.score(head, ("hello world" + i).getBytes());
                Assert.assertEquals(i, score);
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
                        set.add(head, ("hello world" + fj).getBytes(), fj);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(set.contains(head, ("hello world" + i).getBytes()));
                long score = set.score(head, ("hello world" + i).getBytes());
                Assert.assertEquals(i, score);
            }
            Assert.assertEquals(num, set.size(head));

        } finally {
            set.delete(head);
        }


    }

    @Test
    public void add1() throws KitDBException {

        String head = "add0";
        ZSet set = db.getzSet();
        int num = 10 * 10000;


        try {

            FList<ZSet.Entry> entries = new FList<>(num);
            for (int i = 0; i < num; i++) {
                entries.add(new ZSet.Entry(i, ("hello world" + i).getBytes()));
            }

            List<List<ZSet.Entry>> lists = entries.split(100);

            for (List<ZSet.Entry> list : lists) {
                set.add(head, list);
            }

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(set.contains(head, ("hello world" + i).getBytes()));
                long score = set.score(head, ("hello world" + i).getBytes());
                Assert.assertEquals(i, score);
            }

            Assert.assertEquals(num, set.size(head));
        } finally {
            set.delete(head);
        }


        try {

            FList<ZSet.Entry> entries = new FList<>(num);
            for (int i = 0; i < num; i++) {
                entries.add(new ZSet.Entry(i, ("hello world" + i).getBytes()));
            }

            List<List<ZSet.Entry>> lists = entries.split(100);

            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (List<ZSet.Entry> list : lists) {
                joinFuture.add(args -> {
                    try {
                        set.add(head, list);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(set.contains(head, ("hello world" + i).getBytes()));
                long score = set.score(head, ("hello world" + i).getBytes());
                Assert.assertEquals(i, score);
            }

            Assert.assertEquals(num, set.size(head));
        } finally {
            set.delete(head);
        }

    }


    @Test
    public void range() throws KitDBException {
        String head = "range0";
        ZSet set = db.getzSet();
        int num = 10 * 10000;

        try {
            FList<ZSet.Entry> entries = new FList<>(num);
            for (int i = 0; i < num; i++) {
                entries.add(new ZSet.Entry(i, ("hello world" + i).getBytes()));
            }
            set.add(head, entries);
            for (int i = 0; i < 100; i++) {
                List<ZSet.Entry> list = set.range(head, i * 100, (i + 1) * 100, 1000000);

                int j = i * 100;
                for (ZSet.Entry entry : list) {
                    Assert.assertEquals(entry.getScore(), j++);
                }
            }
            int start = 2000;
            int limit = 100;
            List<ZSet.Entry> list = set.range(head, start, Integer.MAX_VALUE, limit);
            Assert.assertEquals(list.size(), limit);
            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(list.get(i).getScore(), 2000 + i);
            }
        } finally {
            set.delete(head);
        }

    }


    @Test
    public void deleteEmpty() throws KitDBException {
        String head = "range0";
        ZSet set = db.getzSet();
        int num = 10 * 10000;
        //set.add(head, ("hello world").getBytes(),10000);

        for (int i = 0; i < num; i++) {
            set.add(head, ("hello world" + i).getBytes(), i);
        }

        for (int i = 0; i < num; i++) {
            set.remove(head, ("hello world" + i).getBytes());
        }
        set.delete(head);
    }


    @Test
    public void rangeDel() throws KitDBException {
        String head = "range0";
        ZSet set = db.getzSet();
        int num = 10 * 10000;

        try {
            FList<ZSet.Entry> entries = new FList<>(num);
            for (int i = 0; i < num; i++) {
                entries.add(new ZSet.Entry(i, ("hello world" + i).getBytes()));
            }
            set.add(head, entries);

            for (int i = 0; i < 1000; i++) {
                List<ZSet.Entry> list = set.rangeDel(head, i * 100, ((i + 1) * 100 - 1), 1000000);

                Assert.assertEquals(list.size(), 100);
                int j = i * 100;
                Assert.assertEquals(set.size(head), num - (i + 1) * 100);

                for (ZSet.Entry entry : list) {
                    Assert.assertEquals(entry.getScore(), j);
                    Assert.assertTrue(!set.contains(head, ("hello world" + j).getBytes()));
                    j++;
                }
            }

        } finally {
            set.delete(head);
        }

    }

    @Test
    public void iterator() throws KitDBException {
        String head = "iterator0";
        ZSet set = db.getzSet();
        int num = 10 * 10000;
        try {
            FList<ZSet.Entry> entries = new FList<>(num);
            for (int i = 0; i < num; i++) {
                entries.add(new ZSet.Entry(i, ("hello world" + i).getBytes()));
            }
            set.add(head, entries);


            FList<ZSet.Entry> entries_2 = new FList<>(num);

            try (RIterator<ZSet> iterator = set.iterator(head)) {
                while (iterator.hasNext()) {
                    ZSet.Entry er = iterator.next();
                    entries_2.add(er);
                }
            }

            Assert.assertEquals(entries_2.size(), entries.size());

            Map<Long, ZSet.Entry> map = entries.toMap(ZSet.Entry::getScore);

            for (ZSet.Entry entry : entries_2) {
                ZSet.Entry entry_1 = map.get(entry.getScore());
                Assert.assertNotNull(entry_1);
                Assert.assertArrayEquals(entry_1.getValue(), entry.getValue());
            }


        } finally {
            set.delete(head);
        }


    }


    @Test
    public void addMayTTL() {

    }

    @Test
    public void addMayTTL1() {

    }


    @Test
    public void getKeyIterator() {


    }

    @Test
    public void deleteFast() throws KitDBException {
        String head = "iterator0";
        ZSet set = db.getzSet();
        int num = 10 * 10000;
        try {
            FList<ZSet.Entry> entries = new FList<>(num);
            for (int i = 0; i < num; i++) {
                entries.add(new ZSet.Entry(i, ("hello world" + i).getBytes()));
            }
            set.add(head, entries);

            set.deleteFast(head);

            Assert.assertTrue(!set.isExist(head));

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(!set.contains(head, ("hello world" + i).getBytes()));
            }

        } finally {
            //set.delete(head);
        }
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
    public void remove() {

    }

    @Test
    public void score() throws KitDBException {

    }

    @Test
    public void score1() {

    }


    @Test
    public void delete() {

    }


    @Test
    public void isExist() {

    }

    @Test
    public void size() {

    }


}