package top.thinkin.lightd.kit;

import java.io.*;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipUtil {
    public static void compressDirectoryToZipFile(final String rootDir, final String sourceDir,
                                                  final ZipOutputStream zos) throws IOException {
        final String dir = Paths.get(rootDir, sourceDir).toString();
        final File[] files = requireNonNull(new File(dir).listFiles(), "files");
        for (final File file : files) {
            if (file.isDirectory()) {
                compressDirectoryToZipFile(rootDir, Paths.get(sourceDir, file.getName()).toString(), zos);
            } else {
                zos.putNextEntry(new ZipEntry(Paths.get(sourceDir, file.getName()).toString()));
                try (final FileInputStream in = new FileInputStream(Paths.get(rootDir, sourceDir, file.getName())
                        .toString())) {
                    copy(in, zos);
                }
            }
        }
    }

    public static int copy(InputStream input, OutputStream output) throws IOException {
        long count = copyLarge(input, output);
        return count > 2147483647L ? -1 : (int) count;
    }


    public static long copyLarge(InputStream input, OutputStream output) throws IOException {
        return copyLarge(input, output, new byte[4096]);
    }


    public static long copyLarge(InputStream input, OutputStream output, byte[] buffer) throws IOException {
        long count = 0L;

        int n;
        for (boolean var5 = false; -1 != (n = input.read(buffer)); count += (long) n) {
            output.write(buffer, 0, n);
        }

        return count;
    }


    public static <T> T requireNonNull(T obj, String message) {
        if (obj == null) {
            throw new NullPointerException(message);
        }
        return obj;
    }

    public static void unzipFile(final String sourceFile, final String outputDir) throws IOException {
        try (final ZipInputStream zis = new ZipInputStream(new FileInputStream(sourceFile))) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                final String fileName = zipEntry.getName();
                final File entryFile = new File(outputDir + File.separator + fileName);
                forceMkdir(entryFile.getParentFile());
                try (final FileOutputStream fos = new FileOutputStream(entryFile)) {
                    copy(zis, fos);
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
    }

    public static void forceMkdir(File directory) throws IOException {
        String message;
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                message = "File " + directory + " exists and is " + "not a directory. Unable to create directory.";
                throw new IOException(message);
            }
        } else if (!directory.mkdirs() && !directory.isDirectory()) {
            message = "Unable to create directory " + directory;
            throw new IOException(message);
        }

    }

    private ZipUtil() {
    }
}
