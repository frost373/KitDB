package top.thinkin.lightd.collect;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tag {
    private byte[] key_b;

    public  static String HEAD = "T";
    public static byte[] HEAD_B = HEAD.getBytes();

    public  static String HEAD_VALUE = "t";
    public static byte[] HEAD_VALUE_B = HEAD_VALUE.getBytes();

    private WriteOptions writeOptions;
    private RocksDB rocksDB;
    private static Charset charset = Charset.forName("UTF-8");
    private final ReadWriteLock readWriteLock        = new ReentrantReadWriteLock();
    private  VersionSequence versionSequence;
    private  ZSet ttlZset;




}
