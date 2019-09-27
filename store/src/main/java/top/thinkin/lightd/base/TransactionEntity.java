package top.thinkin.lightd.base;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.Transaction;

import java.util.HashSet;
import java.util.Set;

@Log4j2
public class TransactionEntity {
    private int count = 0;

    private Transaction transaction;

    private Set<LockEntity> lockEntities = new HashSet<>();

    public Set<String> getLockKeys() {
        return lockKeys;
    }

    private Set<String> lockKeys = new HashSet<>();
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

    public void addLock(LockEntity lockEntity) {
        lockEntities.add(lockEntity);
        lockKeys.add(lockEntity.getKey());
    }


    public void unLock() {
        for (LockEntity lockEntity : lockEntities) {
            try {
                keyDoubletLock.unlock(lockEntity);
            } catch (Exception e) {
                log.error("Tx unlock error", e);
            }
        }
        lockEntities.clear();
        lockKeys.clear();
    }


    public boolean checkKey(String key) {
        return lockKeys.contains(key);
    }

    public void setKeyDoubletLock(KeyDoubletLock keyDoubletLock) {
        this.keyDoubletLock = keyDoubletLock;
    }
}
