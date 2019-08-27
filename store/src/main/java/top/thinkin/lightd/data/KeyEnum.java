package top.thinkin.lightd.data;

public enum KeyEnum {

    RKv_KEY("K"),
    RKv_TTL("T");

    private final String key;


    KeyEnum(String key) {
        this.key = key;
    }


    public String getKey() {
        return key;
    }


}
