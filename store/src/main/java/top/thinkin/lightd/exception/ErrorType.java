package top.thinkin.lightd.exception;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum ErrorType {

    RETAIN_KEY(0, "RETAIN_KEY"),
    DATA_LOCK(1, "DATA_LOCK"),//数据被锁定
    REPEATED_KEY(2, "REPEATED_KEY"),//重复的KEY
    EMPTY(3, "EMPTY"),
    NULL(4, "NULL"),
    STORE_VERSION(5, "STORE_VERSION"), NOT_EXIST(6, "NOT_EXIST"),
    NOT_TX_DB(7, "NOT_TX_DB"), TX_NOT_START(7, "TX_NOT_START");

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
