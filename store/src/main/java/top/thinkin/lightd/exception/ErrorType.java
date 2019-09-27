package top.thinkin.lightd.exception;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum ErrorType {
    STROE_ERROR(-1, "STROE_ERROR"),
    RETAIN_KEY(0, "RETAIN_KEY"),
    DATA_LOCK(1, "DATA_LOCK"),//数据被锁定
    REPEATED_KEY(2, "REPEATED_KEY"),//重复的KEY
    EMPTY(3, "EMPTY"),
    NULL(4, "NULL"),
    STORE_VERSION(5, "STORE_VERSION"), NOT_EXIST(6, "NOT_EXIST"),
    NOT_TX_DB(7, "NOT_TX_DB"), TX_NOT_START(7, "TX_NOT_START"), TX_ERROR(8, "TX_ERROR"), TX_GET_TIMEOUT(9, "TX_GET_TIMEOUT");

    private final String type;
    static final Set<String> ALL;
    static {
        ErrorType[] errorTypes =   ErrorType.values();
        ALL = Arrays.stream(errorTypes).map(e->e.type).collect(Collectors.toSet());
    }
    ErrorType(int key, String type ) {
        this.type = type;
    }

    public static boolean contains(String type){
        return ALL.contains(type);
    }
}
