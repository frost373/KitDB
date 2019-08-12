package top.thinkin.lightd.exception;

public class NonExistException extends Exception{
    private static final long serialVersionUID = 1L;
    private final String target;

    public NonExistException(String target, String errmsg) {
        super(String.join(" ",target,errmsg));
        this.target = target;
    }


    public NonExistException(String target) {
        super(String.join(" ",target,"non-exist"));
        this.target = target;
    }

}
