package top.thinkin.lightd.base;

import java.util.ArrayList;
import java.util.List;

public class TransactionEntity {
    private int count = 0;
    private List<DBCommand> dbCommands = new ArrayList<>(50);


    public int addCount() {
        return count++;
    }

    public int subCount() {
        return count--;
    }

    public void reset() {
        count = 0;
        dbCommands.clear();
    }


    public int getCount() {
        return count;
    }

    public List<DBCommand> getDbCommands() {
        return dbCommands;
    }

    public void add(DBCommand dbCommand) {
        dbCommands.add(dbCommand);
    }
}
