package top.thinkin.lightd.base;


import lombok.AllArgsConstructor;
import lombok.Data;
import org.rocksdb.*;
import top.thinkin.lightd.db.REntry;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
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

    public long index() {
        return index.get();
    }

    public long oldestIndex() {
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

    public synchronized void clear(int num) throws RocksDBException {
        byte[] start_b = ArrayKits.addAll(Long_Key, ArrayKits.longToBytes(oldestIndex));
        byte[] end_b = ArrayKits.addAll(Long_Key, ArrayKits.longToBytes(oldestIndex + num + 1));
        rocksDB.deleteRange(start_b, end_b);
    }


    public synchronized void clearBefore(int index) throws RocksDBException {
        byte[] start_b = ArrayKits.addAll(Long_Key, ArrayKits.longToBytes(oldestIndex));
        byte[] end_b = ArrayKits.addAll(Long_Key, ArrayKits.longToBytes(index));
        rocksDB.deleteRange(start_b, end_b);
    }


    public List<Entry> logs(long start, int limit) {
        List<Entry> entries = new ArrayList<>();
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            byte[] start_b = ArrayKits.addAll(Long_Key, ArrayKits.longToBytes(start));
            iterator.seek(start_b);
            for (int i = 0; iterator.isValid() && i < limit; i++) {
                byte[] key = iterator.key();
                if (!BytesUtil.checkHead(Long_Key, key)) break;
                long index = ArrayKits.bytesToLong(ArrayKits.sub(key, Long_Key.length, key.length - 1));
                Entry entry = new Entry(index, iterator.value());
                entries.add(entry);
            }
        }
        return entries;
    }


    @Data
    @AllArgsConstructor
    public static class Entry extends REntry {
        private long index;
        private byte[] value;
    }

    public BinLog(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
        try (final RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seek(Long_Key);
            if (iterator.isValid()) {
                byte[] start_b = iterator.key();
                if (BytesUtil.checkHead(Long_Key, start_b)) {
                    iterator.seekToLast();
                    byte[] end_b = iterator.key();
                    long start = ArrayKits.bytesToLong(ArrayKits.sub(start_b, Long_Key.length, start_b.length));
                    long end = ArrayKits.bytesToLong(ArrayKits.sub(end_b, Long_Key.length, end_b.length));
                    oldestIndex = start;
                    index.set(end);
                }
            } else {
                oldestIndex = 0;
            }
        }
    }

}
