package top.thinkin.lightd.collect;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class VersionSequence {
    private byte[] key_b = "VersionSequence".getBytes();
    private RocksDB rocksDB;
    protected synchronized int getSequence() throws RocksDBException {
        byte[] value = rocksDB.get(key_b);
        int version;
        if (value == null) {
            version = 0;
        } else {
            version = ArrayKits.bytesToInt(value,0);
        }
        version = version+1;
        if(version == Integer.MAX_VALUE){
            version = 1;
        }

        rocksDB.put(key_b,ArrayKits.intToBytes(version));
        return version;
    }
    protected  VersionSequence(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }
}
