package top.thinkin.lightd.base;

import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.exception.KitDBException;
import top.thinkin.lightd.kit.ArrayKits;


public class VersionSequence {
    private byte[] key_b = "VersionSequence".getBytes();
    private DB db;
    private Integer version;

    public synchronized int incr() throws KitDBException {
        if (version == null) {
            byte[] value = db.simpleGet(key_b, SstColumnFamily.META);
            if (value == null) {
                version = 0;
            } else {
                version = ArrayKits.bytesToInt(value, 0);
            }
        }
        version = version + 1;
        if (version == Integer.MAX_VALUE) {
            version = 1;
        }
        db.simplePut(key_b, ArrayKits.intToBytes(version), SstColumnFamily.META);
        return version;
    }

    public Integer get() {
        return version;
    }

    public VersionSequence(DB db) {
        this.db = db;
    }
}
