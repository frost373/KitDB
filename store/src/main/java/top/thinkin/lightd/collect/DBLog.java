package top.thinkin.lightd.collect;

import cn.hutool.core.util.ArrayUtil;
import lombok.Data;

import java.io.Serializable;

@Data
public class DBLog implements Serializable {
    private static final long serialVersionUID = -1L;

    private DBLogType type;

    private  byte[] key;
    private  byte[] value;

    private  byte[] start;
    private  byte[] end;

    public static DBLog update(byte[] key,byte[] value){
        DBLog dbLog = new DBLog();
        dbLog.key = key;
        dbLog.value = value;
        dbLog.type = DBLogType.UPDATE;
        return dbLog;
    }

    public static DBLog delete(byte[] key) {
        DBLog dbLog = new DBLog();
        dbLog.key = key;
        dbLog.type = DBLogType.DELETE;
        return dbLog;
    }

    public static DBLog deleteRange(byte[] start,byte[] end){
        DBLog dbLog = new DBLog();
        dbLog.start = start;
        dbLog.end = end;
        dbLog.type = DBLogType.DELETE_RANGE;
        return dbLog;
    }


    public byte[] toBytes() {
        switch (this.type) {
            case DELETE:
                return ArrayKits.addAll(ArrayKits.intToBytes(this.type.getKey()), this.key);
            case UPDATE:
                return ArrayKits.addAll(
                        ArrayKits.intToBytes(this.type.getKey()),
                        ArrayKits.intToBytes(this.key.length), this.key,
                        ArrayKits.intToBytes(this.value.length), this.value
                );
            case DELETE_RANGE:
                return ArrayKits.addAll(
                        ArrayKits.intToBytes(this.type.getKey()),
                        ArrayKits.intToBytes(this.start.length), this.start,
                        ArrayKits.intToBytes(this.end.length), this.end);
        }

        return null;
    }


    public static DBLog toLog(byte[] bytes) {

        int type = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, 0, 3), 0);
        switch (type) {
            case 0: //DELETE
                return DBLog.delete(ArrayUtil.sub(bytes, 4, bytes.length - 1));
            case 1: //UPDATE
                return buildUpdate(bytes);
            case 2: //DELETE_RANGE
                return buildDeleteRange(bytes);
        }

        return null;
    }

    private static DBLog buildDeleteRange(byte[] bytes) {
        int start_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, 4, 7), 0);
        int postion = 8;
        byte[] start = ArrayUtil.sub(bytes, postion, postion = (postion + start_length - 1));
        int end_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, postion = postion + 1, postion + 3), 0);
        byte[] end = ArrayUtil.sub(bytes, postion, (postion + end_length - 1));
        return DBLog.deleteRange(start, end);
    }

    private static DBLog buildUpdate(byte[] bytes) {
        int key_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, 4, 7), 0);
        int postion = 8;
        byte[] key = ArrayUtil.sub(bytes, postion, postion = (postion + key_length - 1));
        int value_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, postion = postion + 1, postion + 3), 0);
        byte[] value = ArrayUtil.sub(bytes, postion, (postion + key_length - 1));
        return DBLog.update(key, value);
    }
}
