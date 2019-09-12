package top.thinkin.lightd.db.internal;

import top.thinkin.lightd.data.KeyEnum;
import top.thinkin.lightd.db.TimerStore;

public class CollectTimerStore extends TimerStore {
    @Override
    public byte[] getHead() {
        return KeyEnum.COLLECT_TIMER.getBytes();
    }
}
