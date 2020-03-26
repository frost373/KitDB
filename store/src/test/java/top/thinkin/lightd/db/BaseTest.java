package top.thinkin.lightd.db;

import org.junit.AfterClass;
import org.junit.Before;
import top.thinkin.lightd.exception.KitDBException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BaseTest {
    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors * 8);


    static DB db;

    @Before
    public void init() throws KitDBException {
        String kitdbPath = System.getProperty("kitdb_path", "/data/kitdb");
        if (db == null) {
            db = DB.build(kitdbPath);
        }
    }

    @AfterClass
    public static void after() throws InterruptedException {
        Thread.sleep(5000);
    }
}
