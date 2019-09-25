package top.thinkin.lightd.base;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.Transaction;

import java.util.HashSet;
import java.util.Set;

@Log4j2
public class TransactionEntity {
    private int count = 0;

    private Transaction transaction;

    private Set<KeyDoubletLock.LockEntity> lockEntities = new HashSet<>();

    private KeyDoubletLock keyDoubletLock;

    public int addCount() {
        return count++;
    }

    public int subCount() {
        return count--;
    }

    public void reset() {
        count = 0;
        transaction = null;
    }


    public int getCount() {
        return count;
    }


    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public void addLock(KeyDoubletLock.LockEntity lockEntity) {
        lockEntities.add(lockEntity);
    }

    public void unLock() {
        for (KeyDoubletLock.LockEntity lockEntity : lockEntities) {
            try {
                keyDoubletLock.unlock(lockEntity);
            } catch (Exception e) {
                log.error("Tx unlock error", e);
            }
        }
        lockEntities.clear();
    }

    public void setKeyDoubletLock(KeyDoubletLock keyDoubletLock) {
        this.keyDoubletLock = keyDoubletLock;
    }
}
