package top.thinkin.lightd.base;

public enum DBCommandType {
    DELETE(0, "删除"),
    UPDATE(1, "修改"),
    DELETE_RANGE(2, "批量删除");
    private final int key;
    private final String descp;

    DBCommandType(int key, String descp) {
        this.key = key;
        this.descp  = descp ;
    }

    public int getKey() {
        return key;
    }

    public String getDescp() {
        return descp ;
    }
}
