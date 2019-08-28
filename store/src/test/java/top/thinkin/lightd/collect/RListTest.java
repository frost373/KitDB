/*
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
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.RList;

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
            db = DB.build("D:\\temp\\db", false);
        }
    }

    @Test
    public void add() throws Exception {
        RList list =   db.getList("add");
        try {
            for (int i = 0; i < 1000000; i++) {
                list.add((i + "test").getBytes());
            }
            Assert.assertTrue(list.size() == 1000000);
        } finally {
            list.delete();
        }
    }

    @Test
    public void ttl() throws Exception {
        RList list =   db.getList("ttl");
        try {
            for (int i = 0; i < 100000; i++) {
                list.add((i + "test").getBytes());
            }
            list.ttl(10);
            Thread.sleep(2 * 1000);
            Assert.assertTrue(list.isExist());
            Thread.sleep((9 * 1000));
            Assert.assertTrue(!list.isExist());
        } finally {
            db.clear();
        }
    }


    @Test
    public void pop() throws Exception {
        int k = 100*10000;
        List<byte[]> arrayList = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            arrayList.add((i+"test").getBytes());
        }
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
    }

    @Test
    public void range() throws Exception {
        RList list = db.getList("range");
        try {
            List<byte[]> arrayList = new ArrayList<>();
            for (int i = 0; i < 1000000; i++) {
                arrayList.add((i + "test").getBytes());
            }
            list.addAll(arrayList);
            List<byte[]> listrange =  list.range(50,100);
            int i = 50;
            for (byte[] v:listrange){
                Assert.assertArrayEquals((i + "test").getBytes(), v);
                i++;
            }
        } finally {
            list.delete();
        }
    }

    @Test
    public void get() throws Exception {
        RList list =   db.getList("get");
        try {
            List<byte[]> arrayList = new ArrayList<>();
            for (int i = 0; i < 1000000; i++) {
                arrayList.add((i + "test").getBytes());
            }
            list.addAll(arrayList);
            for (int i = 0; i < 100 * 10000; i++) {
                list.get(i);
                Assert.assertArrayEquals((i + "test").getBytes(), list.get(i));
            }
        } finally {
            list.delete();
        }
    }
}*/
