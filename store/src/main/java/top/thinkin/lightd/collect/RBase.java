package top.thinkin.lightd.collect;

import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class RBase {
    protected byte[] key_b;
    protected DB db;
    protected static Charset charset = Charset.forName("UTF-8");
    private List<DBLog> logs = new ArrayList<>();


    public void start() {
        logs.clear();
    }

    public void commit() throws Exception {
        try (final WriteBatch batch = new WriteBatch()) {
            for (DBLog log : this.logs) {
                switch (log.getType()) {
                    case DELETE:
                        batch.delete(log.getKey());
                        break;
                    case UPDATE:
                        batch.put(log.getKey(), log.getValue());
                        break;
                    case DELETE_RANGE:
                        batch.deleteRange(log.getStart(), log.getEnd());
                        break;
                }
            }
            db.rocksDB().write(db.writeOptions(), batch);
        } catch (Exception e) {
            throw e;
        } finally {
            logs.clear();
        }
    }

    public void release() {
        logs.clear();
    }

    public synchronized void deleteFast() throws Exception {
        MetaAbs metaV = getMetaV();
        deleteFast(this.key_b, this, metaV);
    }

    protected abstract <T extends MetaAbs> T getMetaV() throws Exception;

    protected static void deleteFast(byte[] key_b, RBase rBase, MetaAbs metaV) throws Exception {
        rBase.start();
        try {
            MetaDAbs metaVD = metaV.convertMetaBytes();
            rBase.put(ArrayKits.addAll("D".getBytes(charset), key_b, metaVD.getVersion()), metaVD.toBytes());
            rBase.delete(key_b);
            rBase.commit();
        } finally {
            rBase.release();
        }
    }


    protected void put(byte[] key, byte[] value) {
        logs.add(DBLog.update(key, value));
    }


    protected void delete(byte[] key) {
        logs.add(DBLog.delete(key));
    }

    protected void deleteRange(byte[] start, byte[] end) {
        logs.add(DBLog.deleteRange(start, end));
    }

    protected static void deleteHead(byte[] head, RBase rBase) {
        try (final RocksIterator iterator = rBase.db.rocksDB().newIterator()) {
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
                        rBase.deleteRange(start, end);
                        rBase.delete(end);
                    } else {
                        rBase.delete(start);
                    }
                }
            }
        }
    }


}
