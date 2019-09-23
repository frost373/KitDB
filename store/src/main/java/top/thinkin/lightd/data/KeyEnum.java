package top.thinkin.lightd.data;

public enum KeyEnum {

    KV_KEY("K"), KV_TTL("T"),
    LIST("L"), LIST_VALUE("l"),
    MAP("M"), MAP_KEY("m"),
    SET("S"), SET_V("p"),
    SEQ("U"),
    ZSET("Z"), ZSET_S("z"), ZSET_V("a"),

    COLLECT_TIMER("XCT"), KV_TIMER("XKT");



    private final String key;


    KeyEnum(String key) {
        this.key = key;
    }


    public String getKey() {
        return key;
    }

    public byte[] getBytes() {
        return key.getBytes();
    }
}
