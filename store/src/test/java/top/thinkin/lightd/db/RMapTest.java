package top.thinkin.lightd.db;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import top.thinkin.lightd.exception.KitDBException;

import java.util.*;


@Slf4j
public class RMapTest extends BaseTest {

    @Test
    public void put() throws Exception {
        String head = "put0";
        RMap map = db.getMap();
        int num = 10 * 10000;
        try {
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("world" + i).getBytes());
            }

            for (int i = 0; i < num; i++) {
                byte[] bytes = map.get(head, ("hello" + i));
                Assert.assertEquals(new String(bytes), ("world" + i));
            }
        } finally {
            map.delete(head);
        }
    }

    @Test
    public void putTTL() throws Exception {
        String head = "putTTL0";
        RMap map = db.getMap();
        int num = 10 * 10000;

        try {
            log.debug("start");
            for (int i = 0; i < num; i++) {
                map.putTTL(head, "hello" + i, ("world" + i).getBytes(), 4);
                if (i == num - 10) {
                    Thread.sleep(3000);
                }
            }
            Thread.sleep(2000);
            for (int i = 0; i < num; i++) {
                byte[] bytes = map.get(head, ("hello" + i));
                Assert.assertEquals(new String(bytes), ("world" + i));
            }
            Thread.sleep(2000);

            for (int i = 0; i < num; i++) {
                byte[] bytes = map.get(head, ("hello" + i));
                Assert.assertNull(bytes);
            }
        } finally {
            map.delete(head);
        }
    }

    @Test
    public void putMayTTL() throws Exception {
        String head = "putMayTTL0";
        RMap map = db.getMap();
        int num = 10 * 10000;
        for (int i = 0; i < num; i++) {
            map.putMayTTL(head, 3, "hello" + i, ("world" + i).getBytes());
            if (i == num - 5) {
                map.putMayTTL(head, 300, "hello" + i, ("world" + i).getBytes());
            } else {
                map.putMayTTL(head, 3, "hello" + i, ("world" + i).getBytes());

            }
        }
        Thread.sleep(3000);
        for (int i = 0; i < num; i++) {
            byte[] bytes = map.get(head, ("hello" + i));
            Assert.assertNull(bytes);
        }
    }

    @Test
    public void get() {

    }

    @Test
    public void get1() throws KitDBException {
        String head = "get1";
        RMap map = db.getMap();
        int num = 1000;
        try {
            String[] keys = new String[num];
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("hello" + i).getBytes());
                keys[i] = "hello" + i;
            }
            Map<String, byte[]> my_map = map.get(head, keys);
            Assert.assertEquals(num, my_map.size());
            for (String key : my_map.keySet()) {
                Assert.assertEquals(new String(my_map.get(key)), key);
            }
        } finally {
            map.delete(head);
        }
    }

    @Test
    public void remove() throws KitDBException {
        String head = "remove1";
        RMap map = db.getMap();
        int num = 1000;
        try {
            String[] keys = new String[num];
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("hello" + i).getBytes());
                keys[i] = "hello" + i;
            }
            Random r = new Random(1);

            String[] integers = new String[num / 10];
            for (int i = 0; i < num / 10; i++) {
                integers[i] = "hello" + r.nextInt(num / 10);
            }
            map.remove(head, integers);

            for (String integer : integers) {
                byte[] bytes = map.get(head, integer);
                Assert.assertNull(bytes);
            }

        } finally {
            map.delete(head);
        }
    }

    @Test
    public void delete() throws KitDBException {


    }

    @Test
    public void getKeyIterator() throws KitDBException {
        String head = "getKeyIterator";
        RMap map = db.getMap();
        int num = 100;
        Set<String> keys = new HashSet<>();
        try {
            for (int i = 0; i < num; i++) {
                int num2 = 1000;
                keys.add(head + i);
                for (int i1 = 0; i1 < num2; i1++) {
                    map.put(head + i, "hello" + i, ("world" + i).getBytes());
                }
            }
            Set<String> keys2 = new HashSet<>();
            try (KeyIterator keyIterator = db.getMap().getKeyIterator()) {
                while (keyIterator.hasNext()) {
                    String key = keyIterator.next();
                    keys2.add(key);
                    map.delete(key);
                }
            }
            Assert.assertTrue(keys.containsAll(keys2));
            Assert.assertTrue(keys2.containsAll(keys));
        } finally {
            map.delete(head);
        }
    }

    @Test
    public void deleteFast() throws KitDBException {
        String head = "deleteFast1";
        RMap map = db.getMap();
        int num = 1000;

        for (int i = 0; i < num; i++) {
            map.put(head, "hello" + i, ("world" + i).getBytes());
        }

        map.deleteFast(head);

    }

    @Test
    public void iterator() throws KitDBException {
        String head = "iterator0";
        RMap map = db.getMap();
        int num = 1000;
        try {
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("world" + i).getBytes());
            }
            Map<String, String> map_chack = new HashMap<>();
            try (RIterator<RMap> iterator = map.iterator(head)) {
                while (iterator.hasNext()) {
                    RMap.Entry entry = iterator.next();
                    map_chack.put(entry.getKey(), new String(entry.getValue()));
                }
            } catch (KitDBException e) {
                throw e;
            }

            for (int i = 0; i < num; i++) {
                Assert.assertEquals(("world" + i), map_chack.get("hello" + i));
            }

        } finally {
            map.delete(head);
        }
    }

    @Test
    public void getTtl() {


    }

    @Test
    public void delTtl() throws Exception {
        String head = "delTtl0";
        RMap map = db.getMap();
        int num = 1000;
        try {
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("world" + i).getBytes());
            }
            map.ttl(head, 3);
            map.delTtl(head);
            Thread.sleep(3500);
            for (int i = 0; i < num; i++) {
                byte[] bytes = map.get(head, ("hello" + i));
                Assert.assertEquals(new String(bytes), ("world" + i));
            }
        } finally {
            map.delete(head);
        }
    }

    @Test
    public void ttl() throws Exception {
        String head = "ttl0";
        RMap map = db.getMap();
        int num = 1000;
        try {
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("world" + i).getBytes());
            }
            map.ttl(head, 3);
            Thread.sleep(3500);
            for (int i = 0; i < num; i++) {
                byte[] bytes = map.get(head, ("hello" + i));
                Assert.assertNull(bytes);
            }
        } finally {
            map.delete(head);
        }
    }

    @Test
    public void isExist() throws Exception {
        String head = "isExist0";
        RMap map = db.getMap();
        int num = 1000;
        try {
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("world" + i).getBytes());
            }
            Assert.assertTrue(map.isExist(head));
            map.ttl(head, 3);
            Assert.assertTrue(map.isExist(head));
            Thread.sleep(3000);
            Assert.assertTrue(!map.isExist(head));
            for (int i = 0; i < num; i++) {
                map.put(head, "hello" + i, ("world" + i).getBytes());
            }
            Assert.assertTrue(map.isExist(head));
            map.deleteFast(head);
            Assert.assertTrue(!map.isExist(head));
        } finally {
            map.delete(head);
        }
    }

    @Test
    public void size() {

    }
}