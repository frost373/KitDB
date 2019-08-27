package top.thinkin.lightd.data;

import java.util.Arrays;
import java.util.Collection;

public interface ReservedWords {
    interface ZSET_KEYS {
        String TTL = "sys:TTL";
        Collection<String> ALL = Arrays.asList(TTL);
    }


    interface LIST_KEYS {
        String BINLOG = "sys:BINLOG";
        Collection<String> ALL = Arrays.asList(BINLOG);
    }

}
