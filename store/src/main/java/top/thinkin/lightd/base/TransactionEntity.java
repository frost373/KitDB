package top.thinkin.lightd.base;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.Transaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Log4j2
public class TransactionEntity implements Serializable {
    private int count = 0;
    private String uuid;

    public TransactionEntity() {
        uuid = UUID.randomUUID().toString();
    }

    private transient Transaction transaction;

    private transient List<LockEntity> locks = new ArrayList<>();

    public int addCount() {
        return count++;
    }

    public int subCount() {
        return count--;
    }

    public void reset() {
        count = 0;

        for (LockEntity lock : locks) {
            lock.unlockSelf();
        }

        locks.clear();
        transaction = null;
    }

    public void addLock(LockEntity lockEntity) {
        locks.add(lockEntity);
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


    public String getUuid() {
        return uuid;
    }
}
