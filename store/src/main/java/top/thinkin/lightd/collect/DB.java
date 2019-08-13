package top.thinkin.lightd.collect;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ArrayUtil;
import org.rocksdb.*;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DB {

    private ThreadLocal<AtomHandler> ATOM_HANDLER = new ThreadLocal<>();

    private RocksDB rocksDB;
    private boolean openTransaction = false;

    private  VersionSequence versionSequence;
    private  ZSet ttlZset;
    private final static  byte[] DEL_HEAD = "D".getBytes();
    private  WriteOptions writeOptions;

    static ScheduledThreadPoolExecutor stp =new ScheduledThreadPoolExecutor(3);

    public final ConcurrentHashMap map = new ConcurrentHashMap();

    static {
        RocksDB.loadLibrary();
    }

    protected  void put(byte[] key,byte[] value){
        AtomHandler atomHandler=  ATOM_HANDLER.get();
        Assert.notNull(atomHandler,"AtomHandler is null");
        atomHandler.getLogs().add(DBLog.update(key,value));
    }

    protected   void delete(byte[] key){
        AtomHandler atomHandler=  ATOM_HANDLER.get();
        Assert.notNull(atomHandler,"AtomHandler is null");
        atomHandler.getLogs().add(DBLog.delete(key));
    }

    protected   void deleteRange(byte[] start,byte[] end){
        AtomHandler atomHandler=  ATOM_HANDLER.get();
        Assert.notNull(atomHandler,"AtomHandler is null");
        atomHandler.getLogs().add(DBLog.deleteRange(start,end));
    }

    public   void close(){
        if(stp!=null){
            stp.shutdown();
        }
        if(rocksDB!=null){
            rocksDB.close();
        }
    }


    public   void start(){
        if(ATOM_HANDLER.get()==null){
            ATOM_HANDLER.set(new AtomHandler(0,0));
        }else{
            ATOM_HANDLER.get().addCount();
        }
    }

    public  void commit() throws Exception {
        AtomHandler atomHandler=  ATOM_HANDLER.get();
        Assert.notNull(atomHandler,"AtomHandler is null");
        if(atomHandler.getCount()>0){
            atomHandler.subCount();
            return;
        }
        try (final WriteBatch batch = new WriteBatch()){
            List<DBLog> logs =  atomHandler.getLogs();

            for (DBLog log:logs){
                switch(log.getType()){
                    case DELETE :
                        batch.delete(log.getKey());
                        break;
                    case UPDATE :
                        batch.put(log.getKey(),log.getValue());
                        break;
                    case DELETE_RANGE:
                        batch.deleteRange(log.getStart(),log.getEnd());
                        break;
                }
            }

            rocksDB.write(writeOptions, batch);
        } catch (Exception e) {
            throw e;
        } finally {
            ATOM_HANDLER.remove();
        }
    }

    public  void release(){
        if(ATOM_HANDLER.get()!=null){
            ATOM_HANDLER.remove();
        }
    }


    protected   RocksDB rocksDB(){
        return this.rocksDB;
    }

    protected  ZSet ttlZset(){
        return this.ttlZset;
    }

    protected  VersionSequence versionSequence(){
        return this.versionSequence;
    }
    protected  WriteOptions writeOptions(){
        return this.writeOptions;
    }

    protected void clear(){
        try (RocksIterator iterator = this.rocksDB.newIterator()) {
            iterator.seek(DEL_HEAD);
            while (iterator.isValid()) {
                Thread.sleep(1000);
                byte[] key_bs = iterator.key();
                if(DEL_HEAD[0] != key_bs[0]){
                    break;
                }
                byte[] value =  iterator.value();
                iterator.next();
                byte[] rel_key_bs =  ArrayUtil.sub(key_bs,1,key_bs.length-4);
                if("L".getBytes()[0] == rel_key_bs[0]){
                    RList.delete(rel_key_bs,value,this);
                }

                if ("Z".getBytes()[0] == rel_key_bs[0]) {
                    ZSet.delete(rel_key_bs, value, this);
                }

            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static DB buildTransactionDB(String dir) throws RocksDBException {
        DB db = new DB();
        Options options = new Options();
        options.setCreateIfMissing(true);

        TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
        TransactionDB rocksDB =   TransactionDB.open(options,transactionDBOptions,dir);

        db.rocksDB  = rocksDB;
        db.openTransaction = true;
        db.versionSequence = new VersionSequence(db.rocksDB);
        db.ttlZset = db.getZSet(ReservedWords.ZSET_KEYS.TTL);
        db.writeOptions = new WriteOptions();

        stp.scheduleWithFixedDelay(db::clear, 2, 2, TimeUnit.SECONDS);
        return db;
    }

    public synchronized static DB build(String dir) throws RocksDBException {
        DB db = new DB();
        Options options = new Options();
        options.setCreateIfMissing(true);
       /* OptimisticTransactionDB txnDb =
                OptimisticTransactionDB.open(options, dir);*/
        //TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
       // TransactionDB rocksDB =   TransactionDB.open(options,transactionDBOptions,dir);

        db.rocksDB  = RocksDB.open(options, dir);
        db.versionSequence = new VersionSequence(db.rocksDB);
        db.ttlZset = db.getZSet(ReservedWords.ZSET_KEYS.TTL);
        db.writeOptions = new WriteOptions();

        stp.scheduleWithFixedDelay(db::clear, 2, 2, TimeUnit.SECONDS);
        return db;
    }


    public synchronized RList getList(String key){
        Object list =map.get(RList.HEAD+key);
        if(list == null){
            list = new RList(this,key);
            map.put(RList.HEAD+key,list);
        }
        return (RList) list;
    }


    public synchronized ZSet getZSet(String key) {
        //DAssert.isTrue(ErrorType.contains(key), ErrorType.RETAIN_KEY,"");
        Object zset =map.get(ZSet.HEAD+key);
        if(zset == null){
            zset = new ZSet(this,key);
            map.put(ZSet.HEAD+key,zset);
        }
        return (ZSet) zset;
    }
}
