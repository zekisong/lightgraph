package com.lightgraph.graph.utils;

public class MurmurHashUtils {

    public static int hash(byte[] bytes, int offset, int length, int initval) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int h1 = initval;
        int roundedEnd = offset + (length & 0xfffffffc);

        for (int i = offset; i < roundedEnd; i += 4) {
            int k1 = (bytes[i] & 0xff) | ((bytes[i + 1] & 0xff) << 8) | ((bytes[i + 2] & 0xff) << 16)
                    | (bytes[i + 3] << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);
            h1 = h1 * 5 + 0xe6546b64;
        }
        int k1 = 0;
        switch (length & 0x03) {
            case 3:
                k1 = (bytes[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k1 |= (bytes[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k1 |= (bytes[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);
                k1 *= c2;
                h1 ^= k1;
        }
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
