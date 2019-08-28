package top.thinkin.lightd.db;

import org.rocksdb.Snapshot;

public class RSnapshot implements AutoCloseable {
    private Snapshot snapshot;

    protected RSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public void close() throws Exception {
        if (snapshot != null) {
            snapshot.close();
        }
    }
}
