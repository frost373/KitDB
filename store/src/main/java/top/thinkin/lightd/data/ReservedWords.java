package top.thinkin.lightd.data;

import java.util.Arrays;
import java.util.Collection;

public interface ReservedWords {
    interface ZSET_KEYS {
        String TTL = "sys:TTL";
        String TTL_KV = "sys:TTL_KV";
        String TTL_KV_SOTRE = "sys:TTL_KV_SOTRE";
        Collection<String> ALL = Arrays.asList(TTL, TTL_KV, TTL_KV_SOTRE);
    }


    interface LIST_KEYS {
        String BINLOG = "sys:BINLOG";
        Collection<String> ALL = Arrays.asList(BINLOG);
    }

}
