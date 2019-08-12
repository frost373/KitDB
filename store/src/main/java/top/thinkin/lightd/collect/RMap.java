package top.thinkin.lightd.collect;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDB;

import java.nio.charset.Charset;

public class RMap {
    private byte[] key_b;

    private RocksDB rocksDB;
    private static Charset charset = Charset.forName("UTF-8");


    public static RMap build(RocksDB rocksDB, String key) {
        RMap map = new RMap();
        map.rocksDB = rocksDB;
        map.key_b = ("M" + key).getBytes(charset);
        return map;
    }

    public void put(byte[] key,byte[] value){


    }

    public byte[] get(byte[] key){

        return null;
    }

    public void delete(){

    }

    public void deleteFast(){

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
    public static class Key {
        private int mapKeySize;
        private byte[] mapKey;
        private int keySize;
        private byte[] key;
        private int version;
    }
}
