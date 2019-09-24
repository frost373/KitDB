package top.thinkin.lightd.db;

import cn.hutool.core.util.ArrayUtil;
import lombok.Data;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import top.thinkin.lightd.base.SstColumnFamily;
import top.thinkin.lightd.kit.ArrayKits;
import top.thinkin.lightd.kit.BytesUtil;

import java.util.ArrayList;
import java.util.List;

public class TimerStore {


    public static byte[] DEF = "".getBytes();

    private static byte[] getHead(String headStr) {
        return headStr.getBytes();
    }

    public static void put(RBase rBase, String head, int time, byte[] value) {
        rBase.putDB(ArrayKits.addAll(getHead(head), ArrayKits.intToBytes(time), value), DEF, SstColumnFamily.DEFAULT);
    }

    public static void del(RBase rBase, String head, int time, byte[] value) {
        rBase.deleteDB(ArrayKits.addAll(getHead(head), ArrayKits.intToBytes(time), value), SstColumnFamily.DEFAULT);
    }


    public interface Function {
        void call(List<TData> dataList) throws Exception;
    }

    public static List<TData> rangeDel(DB db, String head, int start, int end, int limit, Function function) throws Exception {
        List<TData> entries = new ArrayList<>();
        List<byte[]> dels = new ArrayList<>();

        try (final RocksIterator iterator = db.newIterator(SstColumnFamily.DEFAULT)) {
            iterator.seek(ArrayKits.addAll(getHead(head), ArrayKits.intToBytes(start)));
            long index = 0;
            int count = 0;
            while (iterator.isValid() && index <= end && count < limit) {
                byte[] key_bs = iterator.key();
                if (!BytesUtil.checkHead(getHead(head), key_bs)) break;
                TData tData = new TData();
                tData.setTime(ArrayKits.bytesToInt(ArrayUtil.sub(key_bs, 3, 7), 0));
                tData.setValue(ArrayUtil.sub(key_bs, 7, key_bs.length));
                index = tData.getTime();
                if (index > end) {
                    break;
                }
                entries.add(tData);
                dels.add(key_bs);
                count++;
                iterator.next();
            }
        }
        function.call(entries);
        try (final WriteBatch batch = new WriteBatch()) {
            for (byte[] del : dels) {
                batch.delete(db.defHandle, del);
            }
            db.rocksDB().write(db.writeOptions, batch);
        } catch (Exception e) {
            throw e;
        }

        return entries;

    }


    public static List<TData> rangeDel(DB db, String head, int start, int end, int limit) throws RocksDBException {
        List<TData> entries = new ArrayList<>();
        List<byte[]> dels = new ArrayList<>();

        try (final RocksIterator iterator = db.newIterator(SstColumnFamily.DEFAULT)) {
            iterator.seek(ArrayKits.addAll(getHead(head), ArrayKits.intToBytes(start)));
            long index = 0;
            int count = 0;
            while (iterator.isValid() && index <= end && count < limit) {
                byte[] key_bs = iterator.key();
                if (!BytesUtil.checkHead(getHead(head), key_bs)) break;
                TData tData = new TData();
                tData.setTime(ArrayKits.bytesToInt(ArrayUtil.sub(key_bs, 3, 7), 0));
                tData.setValue(ArrayUtil.sub(key_bs, 7, key_bs.length));
                index = tData.getTime();
                if (index > end) {
                    break;
                }
                entries.add(tData);
                dels.add(key_bs);
                count++;
                iterator.next();
            }
        }

        try (final WriteBatch batch = new WriteBatch()) {
            for (byte[] del : dels) {
                batch.delete(db.defHandle, del);
            }
            db.rocksDB().write(db.writeOptions, batch);
        } catch (Exception e) {
            throw e;
        }

        return entries;

    }

    @Data
    public static class TData {
        private int time;
        private byte[] value;
    }

}
