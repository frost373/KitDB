/*
package top.thinkin.lightd.collect;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDBException;

import java.nio.charset.Charset;
import java.util.List;

public class RKey {
    public static String HEAD = "K";
    public static byte[] HEAD_TTL = "T".getBytes();

    public static byte[] HEAD_B = HEAD.getBytes();
    protected static Charset charset = Charset.forName("UTF-8");

    private DB db;

    public void set(String key,byte[] value) throws Exception {
        db.start();
        byte[] key_b =  ArrayKits.addAll(HEAD_B,key.getBytes(charset));

        db.putDB(key_b,value);
        db.commit();
    }

    public void setTTL(String key,byte[] value,int ttl) throws Exception {
        db.start();
        byte[] key_b =  ArrayKits.addAll(HEAD_B,key.getBytes(charset));
        db.putDB(ArrayKits.addAll(HEAD_B,key.getBytes(charset)),value);
        db.putDB(ArrayKits.addAll(HEAD_TTL,key.getBytes(charset)),value);
        db.ttlZset().add(key_b,System.currentTimeMillis()/1000+ttl);
        db.commit();
    }


    public void ttl(String key,int ttl) throws Exception {
        db.start();
        byte[] key_b =  ArrayKits.addAll(HEAD_B,key.getBytes(charset));
        //db.putDB(ArrayKits.addAll(HEAD_B,key.getBytes(charset)),value);
        db.ttlZset().add(key_b,System.currentTimeMillis()/1000+ttl);
        db.commit();
    }

    public byte[] get(String key) throws RocksDBException {
        return db.rocksDB().get(ArrayKits.addAll(HEAD_B,key.getBytes(charset)));
    }

    public void del(String key) throws Exception {
        db.start();
        byte[] key_b =  ArrayKits.addAll(HEAD_B,key.getBytes(charset));
        db.deleteDB(key_b);
        db.commit();
    }

    public void delPrefix(String key_){

    }

    public void getPrefix(String key_){

    }

    public List<byte[]> keys(String key_){

        return null;
    }

    */
/**
 * 获取过期时间戳(秒)
 *
 * @return
 * @throws Exception
 * <p>
 * 删除过期时间
 * @return
 * @throws Exception
 * <p>
 * 设置新的过期时间戳(秒)
 * @return
 * @throws Exception
 *//*

    int getTtl() throws Exception{
        return -1;
    }
    */
/**
 * 删除过期时间
 * @return
 * @throws Exception
 *//*

    void delTtl() throws Exception{

    }
    */
/**
 * 设置新的过期时间戳(秒)
 * @return
 * @throws Exception
 *//*

    void ttl(int ttl) throws Exception{

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Value {
        private int timestamp;
        private byte[] value;


    }

}
*/
