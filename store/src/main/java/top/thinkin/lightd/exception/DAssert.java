package top.thinkin.lightd.exception;

import java.util.Collection;
import java.util.Map;

public class DAssert {
    public static void isTrue(boolean val, ErrorType type, String errmsg) throws LightDException {
        if (!val) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void notTrue(boolean val, ErrorType type, String errmsg) throws LightDException {
        if (val) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void isNull(Object val, ErrorType type, String errmsg) throws LightDException {
        if (val == null) {
            return;
        }
        throw new LightDException(type, errmsg);
    }

    public static void notNull(Object val, ErrorType type, String errmsg) throws LightDException {
        if (val == null) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void isEmpty(String val, ErrorType type, String errmsg) throws LightDException {
        if (val != null && val.length() != 0) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void notEmpty(String val, ErrorType type, String errmsg) throws LightDException {
        if (val == null || val.length() == 0) {
            throw new LightDException(type, errmsg);
        }
    }

    @SuppressWarnings("rawtypes")
    public static void isEmpty(Collection val, ErrorType type, String errmsg) throws LightDException {
        if (val != null && val.size() != 0) {
            throw new LightDException(type, errmsg);
        }
    }

    @SuppressWarnings("rawtypes")
    public static void notEmpty(Collection val, ErrorType type, String errmsg) throws LightDException {
        if (val == null || val.size() == 0) {
            throw new LightDException(type, errmsg);
        }
    }

    @SuppressWarnings("rawtypes")
    public static void isEmpty(Map val, ErrorType type, String errmsg) throws LightDException {
        if (val != null && val.size() != 0) {
            throw new LightDException(type, errmsg);
        }
    }

    @SuppressWarnings("rawtypes")
    public static void notEmpty(Map val, ErrorType type, String errmsg) throws LightDException {
        if (val == null || val.size() == 0) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void isEquals(Object val1, Object val2, ErrorType type, String errmsg) throws LightDException {
        if (val1 != val2 && (val1 == null || !val1.equals(val2))) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void notEquals(Object val1, Object val2, ErrorType type, String errmsg) throws LightDException {
        if (val1 == val2 || (val1 != null && val1.equals(val2))) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void minLength(String val, int min, ErrorType type, String errmsg) throws LightDException {
        if (val != null && val.length() < min) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void maxLength(String val, int max, ErrorType type, String errmsg) throws LightDException {
        if (val != null && val.length() > max) {
            throw new LightDException(type, errmsg);
        }
    }

    public static void alphanumeric(String val, ErrorType type, String errmsg) throws LightDException {
        if (val != null) {
            char ch;
            for (int i = 0, n = val.length(); i < n; i++) {
                ch = val.charAt(i);
                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
                    continue;
                }
                throw new LightDException(type, errmsg);
            }
        }
    }

    public static <T> void isContains(Collection<T> c, Object v, ErrorType type, String errmsg) throws LightDException {
        if (c == null || !c.contains(v)) {
            throw new LightDException(type, errmsg);
        }
    }

    public static <T> void notContains(Collection<T> c, Object v, ErrorType type, String errmsg) throws LightDException {
        if (c != null && c.contains(v)) {
            throw new LightDException(type, errmsg);
        }
    }
}
