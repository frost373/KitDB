package top.thinkin.lightd.base;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;

public class Platform {


    private static final boolean IS_WINDOWS = isWindows0();


    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    private static boolean isWindows0() {
        boolean windows = get("os.name", "").toLowerCase(Locale.US).contains("win");
        if (windows) {
            //TODO
            //LOG.debug("Platform: Windows");
        }
        return windows;
    }


    public static String get(final String key, String def) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty.");
        }

        String value = null;
        try {
            if (System.getSecurityManager() == null) {
                value = System.getProperty(key);
            } else {
                value = AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty(key));
            }
        } catch (Exception e) {

        }

        if (value == null) {
            return def;
        }

        return value;
    }
}
