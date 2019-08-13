package top.thinkin.lightd.collect;

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
}
