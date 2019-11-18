package top.thinkin.lightd.kit;



import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileZipUtils {
    private static void zipFile(ZipOutputStream zipOutputStream, File file, String parentFileName) {
        FileInputStream in = null;
        try {
            ZipEntry zipEntry = new ZipEntry(parentFileName + file.getName());
            zipOutputStream.putNextEntry(zipEntry);
            in = new FileInputStream(file);
            int len;
            byte[] buf = new byte[8 * 1024];
            while ((len = in.read(buf)) != -1) {
                zipOutputStream.write(buf, 0, len);
            }
            zipOutputStream.closeEntry();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 递归压缩目录结构
     *
     * @param zipOutputStream
     * @param file
     * @param parentFileName
     */
    private static void directory(ZipOutputStream zipOutputStream, File file, String parentFileName) {
        File[] files = file.listFiles();
        String parentFileNameTemp = null;
        for (File fileTemp :
                files) {
            parentFileNameTemp = StringKits.isEmpty(parentFileName) ? fileTemp.getName() : parentFileName + "/" + fileTemp.getName();
            if (fileTemp.isDirectory()) {
                directory(zipOutputStream, fileTemp, parentFileNameTemp);
            } else {
                zipFile(zipOutputStream, fileTemp, parentFileNameTemp);
            }
        }
    }

    /**
     * 压缩文件目录
     *
     * @param source 源文件目录（单个文件和多层目录）
     * @param destit 目标目录
     */
    public static void zipFiles(String source, String destit) {
        File file = new File(source);
        ZipOutputStream zipOutputStream = null;
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(destit);
            zipOutputStream = new ZipOutputStream(fileOutputStream);
            if (file.isDirectory()) {
                directory(zipOutputStream, file, "");
            } else {
                zipFile(zipOutputStream, file, "");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                zipOutputStream.close();
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    public static boolean delFile(File file) {
        if (!file.exists()) {
            return false;
        }

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                delFile(f);
            }
        }
        return file.delete();
    }
}
