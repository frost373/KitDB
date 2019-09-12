package top.thinkin.lightd.kit;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


    public static boolean noRepeateFinal(List<byte[]> bytess) {
        List<byte[]> temps = new ArrayList<>(bytess.size());

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


    public static class HashEnty {
        public int hashCode;
        public byte[] bytes;
    }

    public static boolean noRepeate(byte[][] bytess) {
        Map<Integer, List<HashEnty>> map = new HashMap<>(bytess.length);
        for (byte[] bytes : bytess) {

            HashEnty hashEnty = new HashEnty();
            hashEnty.hashCode = hashCode(bytes);
            hashEnty.bytes = bytes;
            int hash = hash(hashEnty.hashCode, bytess.length);
            List<HashEnty> list = map.computeIfAbsent(hash, k -> new ArrayList<>());
            list.add(hashEnty);
        }

        for (int key : map.keySet()) {
            List<HashEnty> list = map.get(key);
            if (list.size() > 1) {
                if (!noRepeate2(list)) {
                    return false;
                }
            }
        }

        return true;
    }

    public static boolean noRepeate2(List<HashEnty> bytess) {
        Map<Integer, List<byte[]>> map = new HashMap<>(bytess.size());
        for (HashEnty hashEnty : bytess) {
            int hash = hash(hashEnty.hashCode, bytess.size());
            List<byte[]> list = map.computeIfAbsent(hash, k -> new ArrayList<>());
            list.add(hashEnty.bytes);
        }

        for (int key : map.keySet()) {
            List<byte[]> list = map.get(key);
            if (list.size() > 1) {
                if (!noRepeateFinal(list)) {
                    return false;
                }
            }
        }

        return true;
    }


    private static int hash(Object key, int size) {
        int h;
        return (size - 1) & ((key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16));
    }

    private static int hashCode(byte[] value) {
        int h = 0;
        byte val[] = value;
        for (int i = 0; i < value.length; i++) {
            h = 31 * h + val[i];
        }
        return h;
    }
}
