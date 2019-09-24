package top.thinkin.lightd.base;

import lombok.extern.log4j.Log4j2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import top.thinkin.lightd.benchmark.JoinFuture;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class KeyDoubletLockTest {

    private KeySegmentLockManager keySegmentLockManager;
    private ConcurrentHashMap<String, Integer> map;


    static int availProcessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(availProcessors * 8);

    final Random r = new Random(12345);

    final AtomicLong atomicLong = new AtomicLong(0);

    @Before
    public void init() {
        ScheduledThreadPoolExecutor stp = new ScheduledThreadPoolExecutor(4);
        this.keySegmentLockManager = new KeySegmentLockManager(stp);
    }


    @Test
    public void lock3() {
        KeyDoubletLock keyDoubletLock = keySegmentLockManager.createLock(500);
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        map = new ConcurrentHashMap<>();
        for (int i = 0; i < 9999999; i++) {
            int fj = i;
            joinFuture.add(args -> {
                String key = "hello" + r.nextInt(10 * 10000);
                KeyDoubletLock.LockEntity lockEntity = keyDoubletLock.lock(key);
                Integer ran1 = r.nextInt(10000);
                map.put(key, ran1);

                Assert.assertEquals(ran1, map.get(key));
                keyDoubletLock.unlock(lockEntity);
                long num;
                num = atomicLong.incrementAndGet();
                if (num % 10000 == 0) {
                    log.info("num:" + num);
                    log.info("size:" + keyDoubletLock.getSize());
                }
                return "";
            });
        }
        long startTime = System.currentTimeMillis();
        joinFuture.join();
        long endTime = System.currentTimeMillis();

        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }


    @Test
    public void lock2() {
        SegmentStrLock segmentStrLock = new SegmentStrLock(16);
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        map = new ConcurrentHashMap<>();
        for (int i = 0; i < 9000000; i++) {
            int fj = i;
            joinFuture.add(args -> {
                String key = "hello" + r.nextInt(1000);
                segmentStrLock.lock(key);
                Integer ran1 = r.nextInt(10000);
                map.put(key, ran1);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Assert.assertEquals(ran1, map.get(key));
                segmentStrLock.unlock(key);
                long num;
                num = atomicLong.incrementAndGet();
                if (num % 10000 == 0) {
                    log.info("num:" + num);
                }
                return "";
            });
        }
        long startTime = System.currentTimeMillis();
        joinFuture.join();
        long endTime = System.currentTimeMillis();

        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }
}