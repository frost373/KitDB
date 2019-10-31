package top.thinkin.lightd.raft;

import java.io.File;

public class Util {
    public static boolean delZSPic(String filePath) {
        boolean flag = true;
        if (filePath != null) {
            File file = new File(filePath);
            if (file.exists()) {
                File[] filePaths = file.listFiles();
                for (File f : filePaths) {
                    if (f.isFile()) {
                        f.delete();
                    }
                    if (f.isDirectory()) {
                        String fpath = f.getPath();
                        delZSPic(fpath);
                        f.delete();
                    }
                }
            }
        } else {
            flag = false;
        }
        return flag;
    }

}
