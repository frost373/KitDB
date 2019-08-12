package top.thinkin.lightd.exception;

public class LightDException extends RuntimeException{
    private static final long serialVersionUID = 1L;
    private final ErrorType type;

    public LightDException(ErrorType type, String errmsg) {
        super(String.join(" ", type.name(),errmsg));
        this.type = type;
    }


    public ErrorType getType() {
        return type;
    }
}
