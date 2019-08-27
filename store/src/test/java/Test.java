import top.thinkin.lightd.base.SegmentLock;
import top.thinkin.lightd.benchmark.JoinFuture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    private static SegmentLock lock = new SegmentLock(32);
    static ExecutorService executorService = Executors.newFixedThreadPool(100);

    public static void main(String[] args) {
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        joinFuture.add(a -> {
            lock.lock("hello".getBytes());
            System.out.println("do hello");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("done hello");
            lock.unlock("hello".getBytes());
            return "";
        });

        joinFuture.add(a -> {
            lock.lock("hello".getBytes());
            System.out.println("do hello2");
            System.out.println("done hello2");
            lock.unlock("hello".getBytes());
            return "";
        });

        joinFuture.join();
    }
}
