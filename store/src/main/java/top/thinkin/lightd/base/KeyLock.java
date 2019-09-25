package top.thinkin.lightd.base;

public interface KeyLock {
    LockEntity lock(String key);

    void unlock(LockEntity reentrantLock);
}
