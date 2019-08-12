package top.thinkin.lightd.collect;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RSet  implements  RCollection{


    @Override
    public void delete() throws Exception {

    }

    @Override
    public void deleteFast() throws Exception {

    }

    @Override
    public int getTtl() throws Exception {
        return 0;
    }

    @Override
    public void delTtl() throws Exception {

    }

    @Override
    public void ttl(int timestamp) throws Exception {

    }

    @Override
    public boolean isExist() throws RocksDBException {
        return false;
    }

    @Override
    public int size() throws Exception {
        return 0;
    }

    @Override
    public Entry getEntry(RocksIterator iterator) {
        return null;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Meta{
        private int size;
        private int timestamp;
        private int version;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SData {
        private int mapKeySize;
        private byte[] mapKey;
        private int version;
        private byte[] value;
    }
}
