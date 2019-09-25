package top.thinkin.lightd.base;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TxLock {
    private String key;

    public String getKey() {
        return key;
    }
}
