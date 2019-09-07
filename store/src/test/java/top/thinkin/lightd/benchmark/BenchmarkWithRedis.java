package top.thinkin.lightd.benchmark;

import cn.hutool.core.collection.CollectionUtil;
import org.junit.Assert;
import org.rocksdb.RocksDB;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.RKv;
import top.thinkin.lightd.db.RList;
import top.thinkin.lightd.db.RSet;
import top.thinkin.lightd.kit.ArrayKits;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BenchmarkWithRedis {
    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors);

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        DB db = DB.build("D:\\temp\\db", false);
        for (int i = 0; i < 100; i++) {
            bc(db);
        }


        //db.clear();
        db.close();
        executorService.shutdown();
    }

    private static void bc(DB db) throws Exception {
        RKv SET = db.getrKv();

       /* retry("ZSET", 100, w_times -> sAdd(db.getSet(), w_times));
        retry("ZPOP", 100, w_times -> spop(db.getSet()));
        db.getSet().delete("ZSET");*/

        retry("SET", 100, 1000000, w_times -> set(SET, 1000000));
        retry("GET", 100, 1000000, w_times -> get(SET, 1000000));
        retry("GETNOTTL", 100, 1000000, w_times -> getNoTTL(SET, 1000000));
        RKv kv = db.getrKv();
        retry("incr", 100, w_times -> incr(kv, w_times));

      /*  RList LPUSHs = db.getList();
        retry("LPUSH", 100, w_times -> add(LPUSHs, w_times));
        LPUSHs.deleteFast("LPUSH_LIST");

        RList LPOP_LIST = db.getList();
        addAll("LPOP_LIST", LPOP_LIST, 100);
        retry("LPOP", 100, w_times -> blpop(LPOP_LIST));
        LPOP_LIST.deleteFast("LPOP_LIST");


        RList LRANGE_500 = db.getList();
        addAll("LRANGE_500_LIST", LRANGE_500, 100);
        retry("LRANGE_500", 100, 100000, w_times -> range(LRANGE_500, 100000));
        LRANGE_500.deleteFast("LRANGE_500_LIST");*/

        //retryBatch("MSET", 10, 1000000, w_times -> mset(SET, 1000000, w_times));

 /* RMap MSET_MAP = db.getRMap("MSET_MAP");
        retry("MSET", 100, w_times -> {
            int k = w_times * 10000;
            for (int i = 0; i < k; i++) {
                MSET_MAP.set(ArrayKits.intToBytes(i), ArrayKits.intToBytes(i));
            }
        });
        MSET_MAP.delete();*/

        //db.writeSnapshot();
    }

    private static void range(RList list, int size) {
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int j = 0; j < availProcessors; j++) {
            joinFuture.add(args -> {
                for (int i = 0; i < size / availProcessors; i++) {
                    try {
                        list.range("LRANGE_500_LIST", 0, 450);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return "";
            });
        }
        joinFuture.join();
    }


    private static void mset(RKv rKv, int size, int batckSize) {
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int j = 0; j < availProcessors; j++) {
            int finalJ = j;
            joinFuture.add(args -> {
                int num = 0;
                for (int i = 0; i < size / availProcessors; i++) {
                    num++;
                    List<RKv.Entry> list = new ArrayList<>();
                    list.add(new RKv.Entry(finalJ + ":" + i, ("test" + i).getBytes()));
                    if (num == batckSize) {
                        try {
                            rKv.set(list);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        num = 0;
                    }

                }
                return "";
            });
        }
        joinFuture.join();
    }

    private static void set(RKv rKv, int size) {
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int j = 0; j < availProcessors; j++) {
            int finalJ = j;
            joinFuture.add(args -> {
                for (int i = 0; i < size / availProcessors; i++) {
                    try {
                        rKv.set(finalJ + ":" + i, ("test" + i).getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return "";
            });
        }
        joinFuture.join();
    }

    private static void get(RKv rKv, int size) {
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int j = 0; j < availProcessors; j++) {
            int finalJ = j;
            joinFuture.add(args -> {
                for (int i = 0; i < size / availProcessors; i++) {
                    try {
                        byte[] bytes = rKv.get(finalJ + ":" + i);
                        Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return "";
            });
        }
        joinFuture.join();
    }

    private static void getNoTTL(RKv rKv, int size) {
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        for (int j = 0; j < availProcessors; j++) {
            int finalJ = j;
            joinFuture.add(args -> {
                for (int i = 0; i < size / availProcessors; i++) {
                    try {
                        byte[] bytes = rKv.getNoTTL(finalJ + ":" + i);
                        Assert.assertArrayEquals(bytes, ("test" + i).getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return "";
            });
        }
        joinFuture.join();
    }

    private static void spop(RSet set) throws Exception {
        while (true) {
            java.util.List<byte[]> pops = set.pop("ZSET", 1000);
            if (CollectionUtil.isEmpty(pops)) break;
        }
    }

    private static void blpop(RList list) throws Exception {
        int i = 0;
        while (true) {
            java.util.List<byte[]> pops = list.blpop("LPOP_LIST", 1);
            if (CollectionUtil.isEmpty(pops)) break;
            i++;
        }
    }


    private static void sAdd(RSet set, int w_times) throws Exception {
        int k = w_times * 10000;
        List<byte[]> arrayList = new ArrayList<>(w_times * 10000);
        for (int i = 0; i < k; i++) {
            arrayList.add(ArrayKits.intToBytes(i));
        }
        for (byte[] bytes : arrayList) {
            set.add("ZSET", bytes);
        }
    }


    private static void incr(RKv rKv, int w_times) throws Exception {
        int k = w_times * 10000;
        String key = "incr";
        for (int i = 0; i < k; i++) {
            rKv.incr(key, 1);
        }
    }


    private static void add(RList list, int w_times) throws Exception {
        int k = w_times * 10000;
        List<byte[]> arrayList = new ArrayList<>(w_times * 10000);
        for (int i = 0; i < k; i++) {
            arrayList.add((i + "t").getBytes());
        }
        for (byte[] bytes : arrayList) {
            list.add("LPUSH_LIST", bytes);
        }
    }

    private static void addAll(String name, RList list, int w_times) throws Exception {
        int k = w_times * 10000;
        List<byte[]> arrayList = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            arrayList.add((i + "t").getBytes());
        }
        FList<byte[]> fList = new FList(arrayList);
        List<List<byte[]>> lists = fList.split(10000);
        for (List<byte[]> bytes : lists) {
            list.addAll(name, bytes);
        }
    }


    public static void retry(String name, int w_times, Function function) throws Exception {
        long startTime = System.currentTimeMillis(); //获取开始时间
        function.call(w_times);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("benchmark " + name + ":" + ((w_times * 10000.0) / (endTime - startTime)) * 1000 + " per second"); //输出程序运行时间
    }


    public static void retry(String name, int w_times, int size, Function function) throws Exception {
        long startTime = System.currentTimeMillis(); //获取开始时间
        function.call(w_times);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("benchmark " + name + ":" + ((size * 1.0 / (endTime - startTime)) * 1000 + " per second")); //输出程序运行时间
    }


    public static void retryBatch(String name, int batckSize, int size, Function function) throws Exception {
        long startTime = System.currentTimeMillis(); //获取开始时间
        function.call(batckSize);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("benchmark " + name + ":" + ((size * 1.0 / (endTime - startTime)) * 1000 / batckSize + " per second")); //输出程序运行时间
    }

    public interface Function {
        void call(int w_times) throws Exception;
    }

}
