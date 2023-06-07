package top.thinkin.lightd.db;

import org.rocksdb.RocksDBException;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.exception.DAssert;
import top.thinkin.lightd.exception.ErrorType;
import top.thinkin.lightd.exception.KitDBException;
import top.thinkin.lightd.kit.ArrayKits;

public class Sequence {
    public final static String HEAD = KeyEnum.SEQ.getKey();
    private final static byte[] HEAD_B = HEAD.getBytes();
    private final byte[] key_b;
    private DB db;
    private Long seq;

    private volatile boolean isOpen = true;

    public synchronized long incr(Long increment) throws RocksDBException, KitDBException {
        DAssert.isTrue(isOpen, ErrorType.CLOSE, "sequence is closed");
        if (seq == null) {
            byte[] value = db.rocksDB().get(key_b);
            if (value == null) {
                seq = 0L;
            } else {
                seq = ArrayKits.bytesToLong(value);
            }
        }
        seq = seq + increment;
        db.rocksDB().put(key_b, ArrayKits.longToBytes(seq));
        return seq;
    }

    public Long get() throws KitDBException {
        DAssert.isTrue(isOpen, ErrorType.CLOSE, "sequence is closed");
        return seq;
    }

    protected Sequence(DB db, byte[] key) {
        this.db = db;
        this.key_b = ArrayKits.addAll(HEAD_B, key);
    }

    protected void close() throws KitDBException {
        DAssert.isTrue(isOpen, ErrorType.CLOSE, "sequence is closed");
        isOpen = false;
        db.removeSequence(new String(key_b, db.charset));
    }

    protected void destroy() throws KitDBException, RocksDBException {
        DAssert.isTrue(isOpen, ErrorType.CLOSE, "sequence is closed");
        isOpen = false;
        db.rocksDB().delete(key_b);
        db.removeSequence(new String(key_b, db.charset));
    }
}
