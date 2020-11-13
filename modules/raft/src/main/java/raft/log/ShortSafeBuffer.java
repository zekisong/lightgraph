package raft.log;

import java.nio.ByteBuffer;

public class ShortSafeBuffer {
    private static volatile ThreadLocal<ByteBuffer> TLB = new ThreadLocal<>();

    public static ByteBuffer getTLB() {
        if (TLB.get() == null) {
            synchronized (TLB) {
                if (TLB.get() == null) {
                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
                    TLB.set(buffer);
                }
            }
        }
        ByteBuffer buffer = TLB.get();
        buffer.clear();
        return buffer;
    }
}
