package top.thinkin.lightd.base;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class SegmentStrLock implements KeyLock {
    private final CopyOnWriteArrayList<ReentrantLock> buckets = new CopyOnWriteArrayList();
    private final int size;
    public SegmentStrLock(int size) {
        this.size = size;
        for (int i = 0; i < this.size; i++) {
            buckets.add(new ReentrantLock(true));
        }
    }

    @Override
    public LockEntity lock(String key) {
        int h = hash(key.hashCode());
        ReentrantLock lock = buckets.get(h);
        lock.lock();

        LockEntity lockEntity = new LockEntity(key);
        return lockEntity;
    }

    @Override
    public void unlock(LockEntity reentrantLock) {
        String key = reentrantLock.getKey();
        int h = hash(key.hashCode());
        ReentrantLock lock = buckets.get(h);
        lock.unlock();
    }

    private int hash(Object key) {
        int h;
        return (this.size - 1) & ((key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16));
    }
}
