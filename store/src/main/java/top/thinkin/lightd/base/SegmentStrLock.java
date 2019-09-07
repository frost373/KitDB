package top.thinkin.lightd.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;


public class SegmentStrLock {
    private final List<ReentrantLock> buckets = new ArrayList<>();
    private final int size;
    public SegmentStrLock(int size) {
        this.size = size;
        for (int i = 0; i < this.size; i++) {
            buckets.add(new ReentrantLock(true));
        }
    }

    public void lock(String key) {
        int h = hash(key.hashCode());
        ReentrantLock lock = buckets.get(h);
        lock.lock();
    }

    public void unlock(String key) {
        int h = hash(key.hashCode());
        ReentrantLock lock = buckets.get(h);
        lock.unlock();
    }

    private int hash(Object key) {
        int h;
        return (this.size - 1) & ((key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16));
    }


}
