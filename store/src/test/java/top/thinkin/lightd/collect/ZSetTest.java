/*
package top.thinkin.lightd.collect;

import lombok.extern.log4j.Log4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.ZSet;

import java.util.List;
@Log4j
public class ZSetTest {

    static DB db;

    @Before
    public void init() throws RocksDBException {
        if(db == null){
            RocksDB.loadLibrary();
            db =  DB.build("D:\\temp\\db");
        }

    }

    @Test
    public void add() throws Exception {
        ZSet zSet = null;
        try {

                zSet = db.getZSet("add");

            for (int i = 0; i < 1000; i++) {
                zSet.add(("add"+i).getBytes(),i);
            }

            Assert.assertEquals(new Long(100L),zSet.score(("add"+100).getBytes()));

            System.out.println();


        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(zSet!=null){
                //zSet.deleteDB();
            }
        }

    }

    @Test
    public void add1() throws Exception {

        ZSet zSet=null;
        try {
            zSet = db.getZSet("add1");
            ZSet.REntry[] entrys =  new ZSet.REntry[1000];

            for (int i = 0; i < 1000; i++) {
                ZSet.REntry entry = new ZSet.REntry(i,("add"+i).getBytes());
                entrys[i] = entry;
            }
            zSet.add(entrys);
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(zSet!=null){
                zSet.delete();
            }
        }
    }

    @Test
    public void addMayTTLPrivate() {
    }

    @Test
    public void addMayTTL1() {
    }

    @Test
    public void range() throws Exception {
        ZSet zSet=null;
        try {
            zSet = db.getZSet("range");
            ZSet.REntry[] entrys =  new ZSet.REntry[10000];

            for (int i = 0; i < 10000; i++) {
                ZSet.REntry entry = new ZSet.REntry(i,("range"+i).getBytes());
                entrys[i] = entry;
            }
            zSet.add(entrys);

            for (int i = 0; i < 100; i++) {
                List<ZSet.REntry> list =   zSet.range(i*100,(i+1)*100);
                Assert.assertTrue(list.size()>0);
                int j=i*100;
                for (ZSet.REntry entry:list){
                    Assert.assertEquals(entry.getScore(), j++);
                }
            }
            List<ZSet.REntry> list =   zSet.range(100,Integer.MAX_VALUE);
            Assert.assertTrue(list.size()==10000-100);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(zSet!=null){
                zSet.delete();
            }
        }

    }

    @Test
    public void iterator() {
    }

    @Test
    public void rangeDel() {
    }

    @Test
    public void remove() throws Exception {
        ZSet zSet=null;
        try {
            zSet = db.getZSet("add1");
            ZSet.REntry[] entrys =  new ZSet.REntry[10000];

            for (int i = 0; i < 10000; i++) {
                ZSet.REntry entry = new ZSet.REntry(i,("add"+i).getBytes());
                entrys[i] = entry;
            }
            zSet.add(entrys);
            Assert.assertTrue(zSet.range(100,100).size()==1);
            Assert.assertTrue(zSet.range(900,900).size()==1);

            zSet.remove(("add"+100).getBytes());
            zSet.remove(("add"+900).getBytes());
            Assert.assertTrue(zSet.range(100,100).size()==0);
            Assert.assertTrue(zSet.range(900,900).size()==0);

            zSet.add(("addA"+800).getBytes(),800);
            zSet.add(("addA"+800).getBytes(),800);
            zSet.add(("addA"+800).getBytes(),800);
            Assert.assertTrue(zSet.range(800,800).size()==2);

        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(zSet!=null){
                zSet.delete();
            }
        }

    }

    @Test
    public void score() {
    }

    @Test
    public void score1() {
    }

    @Test
    public void delete() {
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
}*/
