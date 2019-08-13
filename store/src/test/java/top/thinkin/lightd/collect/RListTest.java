package top.thinkin.lightd.collect;


import cn.hutool.core.collection.CollectionUtil;
import lombok.extern.log4j.Log4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Log4j
public class RListTest {
    static DB db;

    @Before
    public void init() throws RocksDBException {
        if(db == null){
            RocksDB.loadLibrary();
            db =  DB.build("D:\\temp\\db");
        }

    }

    //@Test
    public void add() throws Exception {
        long startTime = System.currentTimeMillis(); //获取开始时间

        RList list =   db.getList("add");

        for (int i = 0; i < 1000000; i++) {
            list.add((i+"test").getBytes());
        }
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间

    }

    //@Test
    public void ttl() throws Exception {
        RList list =   db.getList("ttl");
        for (int i = 0; i < 100000; i++) {
            list.add((i+"test").getBytes());
        }
        list.deleteFast();

        Thread.sleep(1000*600);

    }


    // @Test
    public void pop() throws Exception {
        int k = 100*10000;
        List<byte[]> arrayList = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            arrayList.add((i+"test").getBytes());
        }
        long startTime = System.currentTimeMillis(); //获取开始时间
        RList list =   db.getList("pop");
        list.addAll(arrayList);
        try {
            int i=0;
            while (true){
                List<byte[]> pops = list.blpop(100);
                if(CollectionUtil.isEmpty(pops)) break;
                for (byte[] v:pops) {
                    Assert.assertArrayEquals((i+"test").getBytes(),v);
                    i++;
                }
                Assert.assertEquals(list.size(),k-i);
            }
        } finally {
            list.delete();
        }
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间
    }

    //@Test
    public void range() throws Exception {
        List<byte[]> arrayList = new ArrayList<>();

        for (int i = 0; i < 1000000; i++) {
            arrayList.add((i+"test").getBytes());
        }
        RList list =   db.getList("range");

        list.addAll(arrayList);
        long startTime = System.currentTimeMillis(); //获取开始时间


        //for(int i = 0; i < 10000; i++){
            List<byte[]> listrange =  list.range(50,100);
            for (byte[] v:listrange){
                System.out.println(new String(v));
            }
        //}


        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间
        list.delete();
    }

    @Test
    public void get() throws Exception {
        long startTime = System.currentTimeMillis(); //获取开始时间
        RList list =   db.getList("get");
        for (int i = 0; i < 100 * 10000; i++) {
            list.get(i);
            Assert.assertArrayEquals((i + "test").getBytes(), list.get(i));
        }
        for (int i = 0; i < 100 * 10000; i++) {
            list.get(i);
            Assert.assertArrayEquals((i+"test").getBytes(),list.get(i));
        }
        long endTime = System.currentTimeMillis(); //获取结束时间

        System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间
        list.delete();
    }
}