package top.thinkin.lightd.exception;

public class KitDBException extends Exception {
    private static final long serialVersionUID = 1L;
    private final ErrorType type;

    public KitDBException(ErrorType type, String errmsg) {
        super(String.join(" ", type.name(),errmsg));
        this.type = type;
    }


    public KitDBException(ErrorType type, String errmsg, Exception cause) {
        super(String.join(" ", type.name(), errmsg), cause);
        this.type = type;
    }

    public KitDBException(ErrorType type, Exception cause) {
        super(type.name(), cause);
        this.type = type;
    }


    public ErrorType getType() {
        return type;
    }
}
