package top.thinkin.lightd.base;

public enum DBCommandChunkType {
    NOM_COMMIT("普通提交"),
    TX_LOGS("事物操作记录"),
    TX_COMMIT("事物提交"),
    TX_ROLLBACK("事物回滚"), SIMPLE_COMMIT("简单提交");
    private final String descp;

    DBCommandChunkType(String descp) {
        this.descp = descp;
    }
}
