package com.lightgraph.graph.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class ByteUtils {

    public static int RESERVED_BYTE_SIZE＿FOR_TX = 1;
    public static final int SIZE_BYTE = 1;
    public static final int SIZE_SHORT = 2;
    public static final int SIZE_INT = 4;
    public static final int SIZE_LONG = 8;

    private ByteUtils() {
    }

    public static int putBytes(byte[] dest, int off, byte[] src) {
        System.arraycopy(src, 0, dest, off, src.length);
        return off + src.length;
    }

    public static byte[] getBytes(byte[] src, int off, int length) {
        byte[] dest = new byte[length];
        System.arraycopy(src, off, dest, 0, length);
        return dest;
    }

    public static int put(byte[] bytes, int off, byte b) {
        bytes[off] = b;
        return off + SIZE_BYTE;
    }

    public static byte getByte(byte[] bytes, int off) {
        return bytes[off];
    }

    public static int putString(byte[] bytes, int off, String s) {
        for (int i = 0; i < s.length(); i++) {
            bytes[off + i] = (byte) s.charAt(i);
        }
        return off + s.length();
    }

    public static String getString(byte[] bytes, int off, int len) {
        if (bytes == null) {
            return null;
        }
        if (len <= 0) {
            return "";
        }
        return new String(bytes, off, len);
    }

    public static int putLong(byte[] bytes, int offset, long val) {
        for (int i = offset + SIZE_LONG - 1; i > offset; i--) {
            bytes[i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZE_LONG;
    }

    public static byte[] longToBytes(long value) {
        byte[] bytes = new byte[SIZE_LONG];
        putLong(bytes, 0, value);
        return bytes;
    }

    public static long getLong(byte[] bytes, int offset) {
        long l = 0;
        for (int i = offset; i < offset + SIZE_LONG; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    public static int putShort(byte[] bytes, int offset, short val) {
        bytes[offset + 1] = (byte) val;
        val >>= 8;
        bytes[offset] = (byte) val;
        return offset + SIZE_SHORT;
    }

    public static short getShort(byte[] bytes, int offset) {
        short n = 0;
        n ^= bytes[offset] & 0xFF;
        n <<= 8;
        n ^= bytes[offset + 1] & 0xFF;
        return n;
    }

    public static int putInt(byte[] bytes, int offset, int val) {
        for (int i = offset + SIZE_INT - 1; i > offset; i--) {
            bytes[i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZE_INT;
    }

    public static int getInt(byte[] bytes, int offset) {
        int n = 0;
        for (int i = offset; i < (offset + SIZE_INT); i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    public static int readUnsignedVarint(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28) {
                throw illegalVarintException(value);
            }
        }
        value |= b << i;
        return value;
    }

    public static int readVarint(ByteBuffer buffer) {
        int value = readUnsignedVarint(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    public static long readVarlong(ByteBuffer buffer) {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63) {
                throw illegalVarlongException(value);
            }
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    public static void writeUnsignedVarint(int value, ByteBuffer buffer) {
        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            buffer.put(b);
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    public static void writeVarint(int value, ByteBuffer buffer) {
        writeUnsignedVarint((value << 1) ^ (value >> 31), buffer);
    }

    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }

    public static List<byte[]> split(byte[] bytes, byte[] delimiter) {
        List<byte[]> result = new ArrayList<>();
        int pre = 0;
        int pos = 0;
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            if (b == delimiter[pos]) {
                pos++;
            } else {
                pos = 0;
            }
            if (pos == delimiter.length) {
                byte[] item = new byte[i - pre];
                System.arraycopy(bytes, pre, item, 0, item.length);
                result.add(item);
                pre = i + 1;
                pos = 0;
            }
        }
        if (pre != bytes.length) {
            byte[] item = new byte[bytes.length - pre];
            System.arraycopy(bytes, pre, item, 0, item.length);
            result.add(item);
        }
        return result;
    }

    public static byte[] concat(byte[]... items) {
        int len = 0;
        for (byte[] item : items) {
            len = len + item.length;
        }
        byte[] result = new byte[len];
        int pos = 0;
        for (byte[] item : items) {
            System.arraycopy(item, 0, result, pos, item.length);
            pos = pos + item.length;
        }
        return result;
    }

    public static boolean startWith(byte[] bytes, byte[] perfix) {
        if (bytes == null || perfix == null || bytes.length < perfix.length) {
            return false;
        }
        for (int i = 0; i < perfix.length; i++) {
            if (bytes[i] != perfix[i]) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(new String(ByteUtils.concat("123".getBytes(), "456".getBytes(), "789".getBytes())));
    }

    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
