package top.thinkin.lightd.collect;

import cn.hutool.core.util.ArrayUtil;

import java.io.Serializable;
import java.util.Comparator;

public class BytesUtil {

    public static final byte[]               EMPTY_BYTES             = new byte[0];

    // A byte array comparator based on lexicograpic ordering.
    private static final ByteArrayComparator BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

    public static byte[] nullToEmpty(final byte[] bytes) {
        return bytes == null ? EMPTY_BYTES : bytes;
    }

    public static boolean isEmpty(final byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    public static boolean checkHead(byte[] head,byte[] target){
        if(target.length<head.length) return false;
        return getDefaultByteArrayComparator().compare(head, 0, head.length,target,0,head.length)==0;
    }

    public static ByteArrayComparator getDefaultByteArrayComparator() {
        return BYTES_LEXICO_COMPARATOR;
    }

    public static int compare(final byte[] a, final byte[] b) {
        return getDefaultByteArrayComparator().compare(a, b);
    }

    public static byte[] max(final byte[] a, final byte[] b) {
        return getDefaultByteArrayComparator().compare(a, b) > 0 ? a : b;
    }

    public static byte[] min(final byte[] a, final byte[] b) {
        return getDefaultByteArrayComparator().compare(a, b) < 0 ? a : b;
    }

    public interface ByteArrayComparator extends Comparator<byte[]>, Serializable {

        int compare(final byte[] buffer1, final int offset1, final int length1, final byte[] buffer2,
                    final int offset2, final int length2);
    }

    private static class LexicographicByteArrayComparator implements ByteArrayComparator {

        private static final long serialVersionUID = -8623342242397267864L;

        @Override
        public int compare(final byte[] buffer1, final byte[] buffer2) {
            return compare(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
        }

        public int compare(final byte[] buffer1, final int offset1, final int length1, final byte[] buffer2,
                           final int offset2, final int length2) {
            // short circuit equal case
            if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
                return 0;
            }
            // similar to Arrays.compare() but considers offset and length
            final int end1 = offset1 + length1;
            final int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }
    }

    private BytesUtil() {
    }
}
