package top.thinkin.lightd.collect;

import org.rocksdb.RocksIterator;

import java.nio.charset.Charset;

public class RBase {
    protected byte[] key_b;
    protected DB db;
    protected static Charset charset = Charset.forName("UTF-8");

    protected static void deleteHead(byte[] head, DB db) {
        final RocksIterator iterator = db.rocksDB().newIterator();
        iterator.seek(head);
        byte[] start;
        byte[] end;
        if (iterator.isValid()) {
            start = iterator.key();
            iterator.prev();
            if (BytesUtil.checkHead(head, start)) {
                iterator.seekToLast();
                end = iterator.key();
                if (BytesUtil.checkHead(head, end)) {
                    db.deleteRange(start, end);
                    db.delete(end);
                } else {
                    db.delete(start);
                }
            }
        }
    }


}
