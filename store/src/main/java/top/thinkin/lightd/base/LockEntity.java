package top.thinkin.lightd.base;

import java.util.concurrent.locks.ReentrantLock;

public class LockEntity {
    final ReentrantLock reentrantLock = new ReentrantLock();
    private volatile int time;
    private final String key;
    private volatile int lockSize = 0;

    private KeyLock keyLock;

    public LockEntity(String key, KeyLock keyLock) {
        this.key = key;
        this.keyLock = keyLock;
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

    public void unlockSelf() {
        keyLock.unlock(this);
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

    public KeyLock getKeyLock() {
        return keyLock;
    }

    public void setKeyLock(KeyLock keyLock) {
        this.keyLock = keyLock;
    }
}
