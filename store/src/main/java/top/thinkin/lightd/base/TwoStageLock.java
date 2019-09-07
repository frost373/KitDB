package top.thinkin.lightd.base;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TwoStageLock {
    protected ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

}
