/*
package top.thinkin.lightd.collect;

import lombok.extern.log4j.Log4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.RIterator;
import top.thinkin.lightd.db.RSet;

import java.util.ArrayList;
import java.util.List;

@Log4j
public class RSetTest {
    static DB db;

    @Before
    public void init() throws RocksDBException {
        if (db == null) {
            RocksDB.loadLibrary();
            db = DB.build("D:\\temp\\db", false);
        }
    }


    @Test
    public void pop() throws Exception {
        RSet set = db.getSet("pop");
        try {
            for (int i = 0; i < 1000; i++) {
                set.add(("pop" + i + "k").getBytes());
            }
            int i = 0;
            while (true) {
                List<byte[]> pops = set.pop(10);
                if (pops.size() == 0) {
                    break;
                }
                Assert.assertTrue(pops.size() == 10);

                i++;

            }
            Assert.assertEquals(i, 100);
            Assert.assertEquals(set.size(), 0);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            set.delete();
        }


    }


    @Test
    public void remove() throws Exception {
        RSet set = db.getSet("remove");
        int k = 1000;
        try {
            for (int i = 0; i < k; i++) {
                set.add(("pop" + i).getBytes());
            }
            for (int i = 0; i < k; i++) {
                set.remove(("pop" + i).getBytes());
                Assert.assertEquals(set.size(), k - (i + 1));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            set.delete();
        }

        try {
            for (int i = 0; i < k; i++) {
                set.add(("pop" + i).getBytes());
            }
            set.remove(("pop" + 1).getBytes(), ("pop" + 17).getBytes(), ("pop" + 19).getBytes());
            Assert.assertEquals(set.size(), k - (3));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            set.delete();
        }
    }

    @Test
    public void addMayTTLPrivate() throws Exception {
        RSet set = db.getSet("addMayTTLPrivate");
        try {
            set.addMayTTLPrivate(10, ("addMayTTLPrivate" + 1).getBytes(), ("addMayTTLPrivate" + 2).getBytes(), ("addMayTTLPrivate" + 2).getBytes(), ("addMayTTLPrivate" + 3).getBytes());
            Thread.sleep(1000 * 10);
            Assert.assertTrue(!set.isMember(("addMayTTLPrivate" + 1).getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            set.delete();
        }

    }

    @Test
    public void add() throws Exception {
        RSet set = db.getSet("add");
        try {
            set.add(("addMayTTLPrivate" + 1).getBytes(),
                    ("addMayTTLPrivate" + 2).getBytes(), (
                            "addMayTTLPrivate" + 3).getBytes(),
                    ("addMayTTLPrivate" + 4).getBytes());
            Assert.assertTrue(set.isMember(("addMayTTLPrivate" + 1).getBytes()));
            Assert.assertTrue(set.isMember(("addMayTTLPrivate" + 2).getBytes()));
            Assert.assertTrue(set.isMember(("addMayTTLPrivate" + 3).getBytes()));
            Assert.assertTrue(set.isMember(("addMayTTLPrivate" + 4).getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            set.delete();
        }

    }

    @Test
    public void delete() {


    }

    @Test
    public void deleteFast() {
    }

    @Test
    public void iterator() throws Exception {
        RSet set = db.getSet("pop");

        int k = 1000;

        List<String> list = new ArrayList<>();
        try {
            for (int i = 0; i < k; i++) {
                list.add(("pop" + i));
            }

            byte[][] members = new byte[list.size()][];
            for (int i = 0; i < list.size(); i++) {
                members[i] = list.getDB(i).getBytes();
            }

            set.add(members);

            try (RIterator<RSet> iterator = set.iterator()) {
                while (iterator.hasNext()) {
                    RSet.REntry er = (RSet.REntry) iterator.next();
                    list.remove(new String(er.getValue()));
                }
            }

            Assert.assertTrue(list.size() == 0);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            set.delete();
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
    public void isExist() {
    }

    @Test
    public void size() {
    }
}*/
