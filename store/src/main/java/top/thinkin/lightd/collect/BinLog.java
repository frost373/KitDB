package top.thinkin.lightd.collect;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class BinLog {
    private RocksDB rocksDB;
    private final WriteOptions writeOptions = new WriteOptions();

    private AtomicLong index = new AtomicLong(0);
    private volatile long oldestIndex = 0L;

    private final static byte[] Index_Key = "I".getBytes();
    private final static byte[] Long_Key = "L".getBytes();

    private long incr(long increment) {
        return index.addAndGet(increment);
    }

    private synchronized long[] incrs(int num) {
        long[] indexs = new long[num];
        for (int i = 0; i < indexs.length; i++) {
            indexs[i] = incr(1L);
        }
        return indexs;
    }

    public long index() throws RocksDBException {
        return index.get();
    }

    public long oldestIndex() throws RocksDBException {
        return oldestIndex;
    }

    public void addLog(List<byte[]> logs) throws RocksDBException {
        long[] indexs = incrs(logs.size());
        try (final WriteBatch batch = new WriteBatch()) {
            for (int i = 0; i < logs.size(); i++) {
                batch.put(ArrayKits.addAll(Long_Key, ArrayKits.longToBytes(indexs[i])), logs.get(i));
            }
            rocksDB.write(writeOptions, batch);
        }
    }


    public List<byte[]> logs(long index, int limit) {
        return null;
    }

    public BinLog() {

    }

}
