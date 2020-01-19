package top.thinkin.lightd.db;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.base.TxLock;
import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.kit.ArrayKits;

import java.util.ArrayList;
import java.util.List;

public class Index extends RBase {
    public final static String HEAD = KeyEnum.SET.getKey();

    public final static byte[] HEAD_B = HEAD.getBytes();
    public final static byte[] HEAD_V_B = KeyEnum.SET_V.getBytes();


    /**
     * 创建一个索引
     *
     * @param key
     * @param keyTypes
     */
    public void build(String key, List<String> keyTypes) {


    }

    /**
     * 在索引中写入数据
     *
     * @param keys
     * @param value
     */
    public void write(List<byte[]> keys, byte[] value) throws RocksDBException {
        db.rocksDB.put(new IndexLine(keys).getBytes(), value);
    }


    public Query query(List<byte[]> keys) {

        return null;
    }

    @Override
    protected TxLock getTxLock(String key) {
        return null;
    }

    public interface Function {
        void call(List<byte[]> keys, byte[] value);

    }


    public static void main(String[] args) throws RocksDBException {

        Index index = new Index();

        index.write(null, null);

        index.query(null).term(Term.eq(null)).term(Term.eq(null)).query((keys, value) -> {

        });

    }


    public static class Query {
        public Query term(Term term) {
            return this;
        }

        public void query(Function function) {
            function.call(null, null);


        }

        public void query(byte[] value) {


        }
    }

    public static class Term {

        public static Term _() {
            return new Term();
        }

        public static Term eq(byte[] value) {
            return new Term();
        }

        public void range(byte[] start, byte[] end) {

        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class IndexLine {
        private List<byte[]> key = new ArrayList<>();

        public byte[] getBytes() {
            byte[][] bytes = new byte[key.size()][];
            key.toArray(bytes);
            return ArrayKits.addAll(bytes);
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class IndexDot {
        private int keySize;
        private byte[] key;

        public byte[] getBytes() {
            byte[] value = ArrayKits.addAll(ArrayKits.intToBytes(keySize),
                    this.key);
            return value;
        }

        public static IndexDot setKey(byte[] key) {
            return new IndexDot(key.length, key);
        }

    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SData {
        private int mapKeySize;
        private byte[] mapKey;
        private int version;
        private byte[] value;


        public SDataD convertBytes() {
            SDataD sDataD = new SDataD();
            sDataD.setMapKeySize(ArrayKits.intToBytes(this.mapKeySize));
            sDataD.setMapKey(this.mapKey);
            sDataD.setVersion(ArrayKits.intToBytes(this.version));
            sDataD.setValue(this.value);
            return sDataD;
        }


        public byte[] getHead() {
            byte[] value = ArrayKits.addAll(HEAD_V_B, ArrayKits.intToBytes(this.mapKeySize),
                    this.mapKey, ArrayKits.intToBytes(this.version));
            return value;
        }

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SDataD {
        private byte[] mapKeySize;
        private byte[] mapKey;
        private byte[] version;
        private byte[] value;

        public byte[] toBytes() {
            byte[] value = ArrayKits.addAll(HEAD_V_B, this.mapKeySize, this.mapKey, this.version, this.value);
            return value;
        }

        public static SDataD build(byte[] bytes) {
            SDataD sDataD = new SDataD();
            sDataD.setMapKeySize(ArrayKits.sub(bytes, 1, 5));
            int position = ArrayKits.bytesToInt(sDataD.getMapKeySize(), 0);
            sDataD.setMapKey(ArrayKits.sub(bytes, 5, position = 5 + position));
            sDataD.setVersion(ArrayKits.sub(bytes, position, position = position + 4));
            sDataD.setValue(ArrayKits.sub(bytes, position, bytes.length));
            return sDataD;
        }

        public SData convertValue() {
            SData sData = new SData();
            sData.setMapKeySize(ArrayKits.bytesToInt(this.mapKeySize, 0));
            sData.setMapKey(this.mapKey);
            sData.setVersion(ArrayKits.bytesToInt(this.version, 0));
            sData.setValue(this.value);
            return sData;
        }
    }
}
