package top.thinkin.lightd.collect;

import java.util.Arrays;
import java.util.Collection;

public interface ReservedWords {
    interface ZSET_KEYS {
        String TTL = "sys:TTL";
        Collection<String> ALL = Arrays.asList(TTL);
    }

}
