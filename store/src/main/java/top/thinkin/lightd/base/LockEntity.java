package top.thinkin.lightd.base;

import java.util.concurrent.locks.ReentrantLock;

public class LockEntity {
    final ReentrantLock reentrantLock = new ReentrantLock();
    private volatile int time;
    private final String key;
    private volatile int lockSize = 0;

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
