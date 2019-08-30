package top.thinkin.lightd.base;

import lombok.Data;

import java.io.Serializable;

@Data
public class DBCommand implements Serializable {
    private static final long serialVersionUID = -1L;

    private DBCommandType type;

    private SstColumnFamily family;

    private  byte[] key;
    private  byte[] value;

    private  byte[] start;
    private  byte[] end;

    public static DBCommand update(byte[] key, byte[] value, SstColumnFamily family) {
        DBCommand dbCommand = new DBCommand();
        dbCommand.key = key;
        dbCommand.value = value;
        dbCommand.type = DBCommandType.UPDATE;
        dbCommand.family = family;
        return dbCommand;
    }


    public static DBCommand update(byte[] key, byte[] value) {

        return update(key, value, SstColumnFamily.DEFAULT);
    }

    public static DBCommand delete(byte[] key, SstColumnFamily family) {
        DBCommand dbCommand = new DBCommand();
        dbCommand.key = key;
        dbCommand.type = DBCommandType.DELETE;
        dbCommand.family = family;
        return dbCommand;
    }

    public static DBCommand delete(byte[] key) {

        return delete(key, SstColumnFamily.DEFAULT);
    }

    public static DBCommand deleteRange(byte[] start, byte[] end, SstColumnFamily family) {
        DBCommand dbCommand = new DBCommand();
        dbCommand.start = start;
        dbCommand.end = end;
        dbCommand.type = DBCommandType.DELETE_RANGE;
        dbCommand.family = family;
        return dbCommand;
    }

    public static DBCommand deleteRange(byte[] start, byte[] end) {

        return deleteRange(start, end, SstColumnFamily.DEFAULT);
    }


    /*public byte[] toBytes() {
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


    public static DBCommand toLog(byte[] bytes) {

        int type = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, 0, 3), 0);
        switch (type) {
            case 0: //DELETE
                return DBCommand.delete(ArrayUtil.sub(bytes, 4, bytes.length - 1));
            case 1: //UPDATE
                return buildUpdate(bytes);
            case 2: //DELETE_RANGE
                return buildDeleteRange(bytes);
        }

        return null;
    }

    private static DBCommand buildDeleteRange(byte[] bytes) {
        int start_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, 4, 7), 0);
        int postion = 8;
        byte[] start = ArrayUtil.sub(bytes, postion, postion = (postion + start_length - 1));
        int end_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, postion = postion + 1, postion + 3), 0);
        byte[] end = ArrayUtil.sub(bytes, postion, (postion + end_length - 1));
        return DBCommand.deleteRange(start, end);
    }

    private static DBCommand buildUpdate(byte[] bytes) {
        int key_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, 4, 7), 0);
        int postion = 8;
        byte[] key = ArrayUtil.sub(bytes, postion, postion = (postion + key_length - 1));
        int value_length = ArrayKits.bytesToInt(ArrayUtil.sub(bytes, postion = postion + 1, postion + 3), 0);
        byte[] value = ArrayUtil.sub(bytes, postion, (postion + value_length - 1));
        return DBCommand.update(key, value);
    }*/


    public void readLogs() {

    }
}
