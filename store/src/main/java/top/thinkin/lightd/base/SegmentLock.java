package top.thinkin.lightd.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;


public class SegmentLock {
    private final List<ReentrantLock> buckets = new ArrayList<>();
    private final int size;

    public SegmentLock(int size) {
        this.size = size;
        for (int i = 0; i < this.size; i++) {
            buckets.add(new ReentrantLock(true));
        }
    }

    public void lock(byte[] key) {
        int h = hash(hashCode(key));
        ReentrantLock lock = buckets.get(h);
        lock.lock();
    }

    public void unlock(byte[] key) {
        int h = hash(hashCode(key));
        ReentrantLock lock = buckets.get(h);
        lock.unlock();
    }

    private int hash(Object key) {
        int h;
        return (this.size - 1) & ((key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16));
    }

    private int hashCode(byte[] value) {
        int h = 0;
        byte val[] = value;
        for (int i = 0; i < value.length; i++) {
            h = 31 * h + val[i];
        }
        return h;
    }
}
