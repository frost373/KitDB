package top.thinkin.lightd.collect;

import org.rocksdb.RocksIterator;

public class RIterator<R extends RCollection> {
    private final RocksIterator iterator;
    private final RCollection rCollection;
    private final byte[] seekHead;
    private  boolean finish = false;

    public  RIterator(RocksIterator iterator,RCollection rCollection,byte[] seekHead){
        this.iterator = iterator;
        this.rCollection = rCollection;
        this.seekHead = seekHead;
    }

    public boolean hasNext(){
        if(finish) return false;
        byte[] key = iterator.key();
        if(key==null||!BytesUtil.checkHead(seekHead,key)){
            finish = true;
            return false;
        }
        return iterator.isValid();
    }


    public R.Entry next(){
        R.Entry entry =  rCollection.getEntry(iterator);
        iterator.next();
        return entry;
    }

    public byte[] getSeek() {
        return seekHead;
    }
}
