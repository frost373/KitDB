package top.thinkin.lightd.base;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Key双层锁
 * 由短锁和长锁两层锁组成，期望在控制复杂度的情况下，达到最大的并发能力
 * 1.短锁：Key分段锁
 * 2.长锁：Key锁
 */
public class KeyDoubletLock {

    private final ConcurrentHashMap<String, LockEntity> map;
    protected final SegmentStrLock lock;
    private int maxSize;

    public int getSize() {
        return map.size();
    }

    public KeyDoubletLock(int maxSize) {
        this.maxSize = maxSize;
        this.lock = new SegmentStrLock(16);
        this.map = new ConcurrentHashMap<>(maxSize * 3);
    }

    public static class LockEntity {
        final ReentrantLock reentrantLock = new ReentrantLock();
        private volatile int time;
        private final String key;
        private int lockSize = 0;

        public LockEntity(String key) {
            this.key = key;
        }

        public void lock() {
            reentrantLock.lock();
        }

        public void unlock() {
            reentrantLock.unlock();
        }

        public void setTime() {
            time = (int) (System.currentTimeMillis() / 1000);
        }

        public int getTime() {
            return time;
        }


        public String getKey() {
            return key;
        }

        public synchronized void addSize() {
            this.lockSize++;
        }

        public synchronized void subSize() {
            lockSize--;
        }

        public synchronized int getLockSize() {
            return lockSize;
        }
    }


    public LockEntity lock(String key) {
        LockEntity reentrantLock;
        lock.lock(key);
        try {
            reentrantLock = map.get(key);
            if (reentrantLock == null) {
                reentrantLock = new LockEntity(key);
                map.put(key, reentrantLock);
            }
            reentrantLock.addSize();
        } finally {
            lock.unlock(key);
        }
        reentrantLock.lock();
        reentrantLock.setTime();
        return reentrantLock;
    }

    public void unlock(LockEntity reentrantLock) {
        lock.lock(reentrantLock.getKey());
        try {
            reentrantLock.unlock();
            reentrantLock.subSize();
            if (map.size() > (maxSize * 5)) {
                LockEntity reentrantLock_get = map.get(reentrantLock.getKey());
                if (reentrantLock.equals(reentrantLock_get) && reentrantLock.getLockSize() == 0) {
                    map.remove(reentrantLock.getKey());
                }
            }
        } finally {
            lock.unlock(reentrantLock.getKey());
        }
    }


    private void clear(int time, String key) {
        lock.lock(key);
        try {
            LockEntity reentrantLock = map.get(key);
            if (reentrantLock != null && (time - reentrantLock.getTime()) > 30 && reentrantLock.getLockSize() == 0) {
                map.remove(key);
            }
        } finally {
            lock.unlock(key);
        }
    }

    public void clear() {
        if (map.size() <= maxSize) {
            return;
        }
        Iterator<String> iter = map.keySet().iterator();
        int time = (int) (System.currentTimeMillis() / 1000);
        while (iter.hasNext()) {
            String key = iter.next();
            clear(time, key);
        }
    }
}
