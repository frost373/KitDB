package top.thinkin.lightd.base;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.Transaction;

@Log4j2
public class TransactionEntity {
    private int count = 0;

    private Transaction transaction;

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


}
