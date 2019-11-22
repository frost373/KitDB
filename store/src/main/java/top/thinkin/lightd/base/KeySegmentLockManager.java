package top.thinkin.lightd.base;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class KeySegmentLockManager {
    private List<KeyDoubletLock> list = new CopyOnWriteArrayList<>();

    public KeyDoubletLock createLock(int maxSize) {
        KeyDoubletLock keyDoubletLock = new KeyDoubletLock(maxSize);
        list.add(keyDoubletLock);
        return keyDoubletLock;
    }

    public KeySegmentLockManager(ScheduledThreadPoolExecutor stp) {
        stp.scheduleWithFixedDelay(this::check, 30, 30, TimeUnit.SECONDS);
    }

    public void start(ScheduledThreadPoolExecutor stp) {
        stp.scheduleWithFixedDelay(this::check, 30, 30, TimeUnit.SECONDS);
    }

    public void check() {
        for (KeyDoubletLock keyDoubletLock : list) {
            keyDoubletLock.clear();
        }
    }
}
