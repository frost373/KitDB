package top.thinkin.lightd.base;

import lombok.AllArgsConstructor;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;

@AllArgsConstructor
public class CloseLock implements Closeable {

    private Lock lock;

    @Override
    public void close() {
        try {
            lock.unlock();
        } catch (Exception e) {
            throw e;
        }
    }
}
