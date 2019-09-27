package top.thinkin.lightd.db;

import cn.hutool.core.collection.CollUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.base.TxLock;
import top.thinkin.lightd.benchmark.JoinFuture;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class RListTest {
    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors * 8);
    static DB db;

    @Before
    public void init() throws RocksDBException {
        if (db == null) {
            RocksDB.loadLibrary();
            db = DB.buildTransactionDB("D:\\temp\\db", true);
        }
    }


    @After
    public void after() throws InterruptedException {
        Thread.sleep(5000);
    }


    @Test
    public void add() throws Exception {
        String head = "setA";
        RList list = db.getList();
        int num = 100 * 10000;
        try {

            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }

            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertEquals(new String(bytes), ("hello" + i));
            }
        } catch (Exception e) {
            list.delete(head);
        }

        try {
            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (int i = 0; i < num; i++) {
                int fj = i;
                joinFuture.add(args -> {
                    try {
                        list.add(head, ("hello" + fj).getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();
            Set<String> sets = new HashSet<>();
            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                sets.add(new String(bytes));
            }

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(sets.contains("hello" + i));
            }
        } finally {
            list.delete(head);
        }

    }

    @Test
    public void addAll() throws Exception {
        String head = "addAllA";
        RList list = db.getList();
        int num = 10;
        List<byte[]> integers = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            integers.add(("hello" + i).getBytes());
        }

        List<List<byte[]>> integers_splits = CollUtil.split(integers, 3000);
        try {

            for (List<byte[]> integers_split : integers_splits) {
                list.addAll(head, integers_split);
            }
            Assert.assertEquals(num, list.size(head));
            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertEquals(new String(bytes), ("hello" + i));
            }
        } finally {
            list.delete(head);
        }

        //并发写入测试
        try {
            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (List<byte[]> integers_split : integers_splits) {
                joinFuture.add(args -> {
                    try {
                        list.addAll(head, integers_split);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();
            Assert.assertEquals(num, list.size(head));

            Set<String> sets = new HashSet<>();
            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                sets.add(new String(bytes));
            }

            for (int i = 0; i < num; i++) {
                Assert.assertTrue(sets.contains("hello" + i));
            }
        } finally {
            list.delete(head);
        }
    }


    @Test
    public void size() throws Exception {
        String head = "sizeA";
        RList list = db.getList();
        int num = 10 * 10000;
        try {
            JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
            for (int i = 0; i < num; i++) {
                int fj = i;
                joinFuture.add(args -> {
                    try {
                        list.add(head, ("hello" + fj).getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                });
            }
            joinFuture.join();

            Assert.assertEquals(num, list.size(head));

            int remove_num = 0;
            int[] nums = TestUtil.randomArray(1, 123, 100);

            for (int i : nums) {
                list.blpop(head, i);
                remove_num = remove_num + i;
            }
            Assert.assertEquals(num - remove_num, list.size(head));
        } finally {
            list.delete(head);
        }
        Assert.assertEquals(0, list.size(head));


    }


    @Test
    public void get() {
        //Not a necessary test
    }

    @Test
    public void get1() throws Exception {
        String head = "get1";
        RList list = db.getList();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }
            List<Long> integers = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                integers.add((long) i);
            }
            List<List<Long>> integers_splits = CollUtil.split(integers, 3000);
            for (List<Long> integers_split : integers_splits) {
                List<byte[]> bytess = list.get(head, integers_split);
                for (int i = 0; i < bytess.size(); i++) {
                    Assert.assertEquals(new String(bytess.get(i)), ("hello" + integers_split.get(i)));
                }
            }
        } finally {
            list.delete(head);
        }

    }

    @Test
    public void iterator() throws Exception {
        String head = "iterator0";
        RList list = db.getList();
        int num = 100 * 10000;

        try {
            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }

           /* executorService.submit(()->{
                System.out.println("start");
                try {
                    for (int i = 0; i < num; i++) {

                        list.set(head,i,("hello2222222"+(num+i)).getBytes());
                    }

                    System.out.println("stop");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return "";
            });*/

            long startTime = System.currentTimeMillis();
            try (RIterator<RList> iterator = list.iterator(head)) {

                while (iterator.hasNext()) {
                    RList.Entry er = iterator.next();
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println("程序运行时间：" + (endTime - startTime) + "ms");

            long startTime2 = System.currentTimeMillis();
            for (int i = 0; i < num; i++) {
                list.get(head, i);
            }
            long endTime2 = System.currentTimeMillis();
            System.out.println("程序运行时间：" + (endTime2 - startTime2) + "ms");

            Thread.sleep(2000);
            /*try (RIterator<RList> iterator = list.iterator(head)) {
                while (iterator.hasNext()) {
                    RList.Entry er  =  iterator.next();
                    //Thread.sleep(100);
                    System.out.println("2 index:"+er.getIndex()+" value:"+new String(er.getValue()));
                }
            }*/
        } finally {
            list.delete(head);
        }
    }


   /* @Test
    public void iterator2() throws Exception {
        String head = "iterator0";
        RList list = db.getList();
        try (RIterator<RList> iterator = list.iterator(head)) {
            while (iterator.hasNext()) {
                RList.Entry er = iterator.next();
                //Thread.sleep(100);
                System.out.println("index:" + er.getIndex() + " value:" + new String(er.getValue()));
            }
        }

    }*/

    @Test
    public void range() throws Exception {
        String head = "range0";
        RList list = db.getList();
        int num = 10 * 10000;

        for (int i = 0; i < num; i++) {
            list.add(head, ("hello" + i).getBytes());
        }
        int i = 1234;
        List<byte[]> rangs = list.range(head, i, i + i);

        for (int j = 0; j < i; j++) {
            Assert.assertEquals(("hello" + (i + j)), new String(rangs.get(j)));
        }

    }


    @Test
    public void blpop() throws Exception {
        String head = "blpop0";
        RList list = db.getList();
        int num = 10000;
        try {
            for (int k = 0; k < 10; k++) {
                for (int i = 0; i < num; i++) {
                    list.add(head, ("hello" + i).getBytes());
                }

                JoinFuture<List> joinFuture = JoinFuture.build(executorService, List.class);
                for (int i = 0; i < 4; i++) {
                    joinFuture.add(args -> {
                        List<byte[]> listJoin = new ArrayList<>();
                        try {
                            while (true) {
                                db.startTran(new TxLock(head));
                                List<byte[]> pops = list.blpop(head, 1000);
                                Thread.sleep(50);
                                db.commitTX();
                                if (pops.size() == 0) {
                                    break;
                                }
                                listJoin.addAll(pops);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return listJoin;
                    });
                }
                List<List> joins = joinFuture.join();
                List<String> all = new ArrayList<>();
                for (List<byte[]> join : joins) {
                    for (byte[] bytes : join) {
                        all.add(new String(bytes));
                    }
                }
                Assert.assertEquals(num, all.size());
                Set<String> sets = new HashSet<>(all);
                for (int i = 0; i < num; i++) {
                    Assert.assertTrue(sets.contains("hello" + i));
                }

            }

        } finally {
            list.delete(head);
        }

    }

    @Test
    public void tx() throws Exception {
        String head = "tx0";
        RList list = db.getList();
        try {
            JoinFuture<List> joinFuture = JoinFuture.build(executorService, List.class);
            for (int i = 0; i < 4; i++) {
                joinFuture.add(args -> {
                    List<byte[]> listJoin = new ArrayList<>();
                    try {
                        int num = 1;
                        for (int j = 0; j < num; j++) {
                            db.startTran(list.getTxLock(head + "A"));
                            list.add(head + "A", ("hello" + j).getBytes());
                            db.commitTX();
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return listJoin;
                });
            }
            List<List> joins = joinFuture.join();
        } finally {
            list.delete(head + "A");
            list.delete(head + "B");
        }
    }
    @Test
    public void brpop() throws Exception {
        String head = "brpop0";
        RList list = db.getList();
        try {
            int num = 10 * 10000;
            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }

            int i1 = 1000;
            int count = 0;
            for (int i = 0; i < (num / i1); i++) {
                List<byte[]> pops = list.brpop(head, i1);
                Assert.assertEquals(i1, pops.size());
                Assert.assertEquals(num - (i1 * (i + 1)), list.size(head));
                for (int j = 0; j < i1; j++) {
                    count++;
                    Assert.assertEquals(("hello" + (num - count)), new String(pops.get(j)));
                }
            }


            for (int k = 0; k < 10; k++) {
                for (int i = 0; i < num; i++) {
                    list.add(head, ("hello" + i).getBytes());
                }

                JoinFuture<List> joinFuture = JoinFuture.build(executorService, List.class);
                for (int i = 0; i < 4; i++) {
                    joinFuture.add(args -> {
                        List<byte[]> listJoin = new ArrayList<>();
                        try {
                            while (true) {
                                List<byte[]> pops = list.brpop(head, 1000);
                                if (pops.size() == 0) {
                                    break;
                                }
                                listJoin.addAll(pops);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return listJoin;
                    });
                }
                List<List> joins = joinFuture.join();
                List<String> all = new ArrayList<>();
                for (List<byte[]> join : joins) {
                    for (byte[] bytes : join) {
                        all.add(new String(bytes));
                    }
                }
                Assert.assertEquals(num, all.size());
                Set<String> sets = new HashSet<>(all);
                for (int i = 0; i < num; i++) {
                    Assert.assertTrue(sets.contains("hello" + i));
                }

            }


        } finally {
            list.delete(head);
        }
    }

    @Test
    public void isExist() throws Exception {
        String head = "isExist0";
        RList list = db.getList();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }
            Assert.assertTrue(list.isExist(head));
            list.delete(head);
            Assert.assertTrue(!list.isExist(head));

            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }
            list.ttl(head, 1);
            Assert.assertTrue(list.isExist(head));
            Thread.sleep(1000);
            Assert.assertTrue(!list.isExist(head));
        } finally {
            list.delete(head);
        }
    }

    @Test
    public void ttl() throws Exception {
        String head = "isExist0";
        RList list = db.getList();
        int num = 10000;
        try {
            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }

            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertEquals(new String(bytes), ("hello" + i));
            }

            list.ttl(head, 1);
            list.ttl(head, 2);
            list.ttl(head, 3);
            list.ttl(head, 5);
            Thread.sleep(3000);
            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertEquals(new String(bytes), ("hello" + i));
            }

            Thread.sleep(2000);
            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertNull(bytes);
            }

        } finally {
            //list.delete(head);
        }
    }

    @Test
    public void delTtl() throws Exception {
        String head = "delTtl0";
        RList list = db.getList();
        int num = 1;
        try {
            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }

            list.ttl(head, 1);
            list.delTtl(head);
            Thread.sleep(6000);
            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertEquals(new String(bytes), ("hello" + i));
            }
        } finally {
            list.delete(head);
        }

    }

    @Test
    public void getTtl() {

    }

    @Test
    public void delete() {
        //Not a necessary test
    }


    @Test
    public void addAllMayTTL() throws Exception {
        String head = "addAllMayTTL0";
        RList list = db.getList();
        int num = 10;
        List<byte[]> integers = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            integers.add(("hello" + i).getBytes());
        }

        List<List<byte[]>> integers_splits = CollUtil.split(integers, 3000);
        try {

            for (List<byte[]> integers_split : integers_splits) {
                list.addAllMayTTL(head, integers_split, 5);
            }
            Assert.assertEquals(num, list.size(head));
            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertEquals(new String(bytes), ("hello" + i));
            }
            Thread.sleep(5000);

            Assert.assertEquals(0, list.size(head));


            for (int i = 0; i < num; i++) {
                byte[] bytes = list.get(head, i);
                Assert.assertNull(bytes);
            }
        } finally {
            //list.delete(head);
        }
    }

    @Test
    public void deleteFast() throws Exception {
        String head = "deleteFast0";
        RList list = db.getList();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                list.add(head, ("hello" + i).getBytes());
            }
            list.deleteFast(head);
            Thread.sleep(5000);
        } finally {
            // list.delete(head);
        }
    }

    @Test
    public void addMayTTL() {

    }


    @Test
    public void set() {
        //Not a necessary test
    }


}