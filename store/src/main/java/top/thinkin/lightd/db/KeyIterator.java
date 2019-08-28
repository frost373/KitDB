package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import org.rocksdb.RocksIterator;
import top.thinkin.lightd.kit.BytesUtil;

public class KeyIterator implements AutoCloseable {
    private final RocksIterator iterator;
    private final byte[] seekHead;
    private boolean finish = false;
    private byte[] now;
    private byte[] next;

    public KeyIterator(RocksIterator iterator, byte[] seekHead) {
        this.iterator = iterator;
        this.seekHead = seekHead;
    }

    public boolean hasNext() {
        if (finish) return false;
        byte[] key = iterator.key();
        if (key == null || !BytesUtil.checkHead(seekHead, key)) {
            finish = true;
            return false;
        }
        return iterator.isValid();
    }


    public String next() {
        if (!iterator.isValid()) return null;
        byte[] cKey = iterator.key();
        if (cKey == null) {
            return null;
        }
        iterator.next();
        return new String(ArrayUtil.sub(cKey, 1, cKey.length), RBase.charset);
    }

    public byte[] getSeek() {
        return seekHead;
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }
}
