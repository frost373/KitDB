package top.thinkin.lightd.base;

public enum SstColumnFamily {

    DEFAULT(0), META(1);

    private final int value;

    SstColumnFamily(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
