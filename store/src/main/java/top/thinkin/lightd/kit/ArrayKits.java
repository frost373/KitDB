package top.thinkin.lightd.kit;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ArrayKits {
    public static byte[] newArray(Class<?> componentType, int newSize) {
        return (byte[]) Array.newInstance(componentType, newSize);
    }

    public static byte[] addAll(byte[]... arrays) {
        if (arrays.length == 1) {
            return arrays[0];
        }

        int length = 0;
        for (byte[] array : arrays) {
            if (array == null) {
                continue;
            }
            length += array.length;
        }
        byte[] result = newArray(arrays.getClass().getComponentType().getComponentType(), length);

        length = 0;
        for (byte[] array : arrays) {
            if (array == null) {
                continue;
            }
            System.arraycopy(array, 0, result, length, array.length);
            length += array.length;
        }
        return result;
    }


    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static byte[] intToBytes( int value )
    {
        byte[] src = new byte[4];
        src[3] =  (byte) ((value>>24) & 0xFF);
        src[2] =  (byte) ((value>>16) & 0xFF);
        src[1] =  (byte) ((value>>8) & 0xFF);
        src[0] =  (byte) (value & 0xFF);
        return src;
    }

    public static int bytesToInt(byte[] src, int offset) {
        int value;
        value = (src[offset] & 0xFF)
                | ((src[offset+1] & 0xFF)<<8)
                | ((src[offset+2] & 0xFF)<<16)
                | ((src[offset+3] & 0xFF)<<24);
        return value;
    }


    public static boolean noRepeate(byte[][] bytess) {
        List<byte[]> temps = new ArrayList<>(bytess.length);

        for (byte[] bytes : bytess) {
            for (byte[] temp : temps) {
                if (BytesUtil.compare(temp, bytes) == 0) {
                    return false;
                }
            }
            temps.add(bytes);
        }
        return true;
    }
}
