package top.thinkin.lightd.db;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.exception.KitDBException;

@Slf4j
public class SequenceTest {
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


    @Test
    public void incr() throws KitDBException, RocksDBException {
        Sequence sequence = db.getSequence("hello");
        try {
            for (int i = 0; i < 1000000; i++) {
                Assert.assertEquals(i + 1, sequence.incr(1L));
            }
        } finally {
            sequence.destroy();
        }
    }

    @Test
    public void get() throws KitDBException, RocksDBException {
        Sequence sequence = db.getSequence("hello");
        try {
            for (int i = 0; i < 1000000; i++) {
                Assert.assertEquals(i + 1, sequence.incr(1L));
            }
        } finally {
            sequence.destroy();
        }
    }

    @Test
    public void close() {
    }

    @Test
    public void destroy() {
    }
}