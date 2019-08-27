package top.thinkin.lightd.base;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.kit.ArrayKits;


public class VersionSequence {
    private byte[] key_b = "VersionSequence".getBytes();
    private RocksDB rocksDB;
    private Integer version;

    public synchronized int incr() throws RocksDBException {
        if (version == null) {
            byte[] value = rocksDB.get(key_b);
            if (value == null) {
                version = 0;
            } else {
                version = ArrayKits.bytesToInt(value, 0);
            }
        }
        version = version+1;
        if(version == Integer.MAX_VALUE){
            version = 1;
        }
        rocksDB.put(key_b,ArrayKits.intToBytes(version));
        return version;
    }

    public Integer get() {
        return version;
    }

    public VersionSequence(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }
}
