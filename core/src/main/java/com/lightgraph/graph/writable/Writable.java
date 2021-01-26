package com.lightgraph.graph.writable;

import com.lightgraph.graph.utils.SerdeUtils;
import java.beans.Transient;

public abstract class Writable {


    public <T> byte[] getBytes(Class<T> clazz) {
        return SerdeUtils.getBytes((T) this, clazz);
    }

    @Transient
    public byte[] getBytes() {
        return SerdeUtils.getBytes(this);
    }

    public static <T> T getInstance(byte[] bytes, Class<T> clazz) {
        return SerdeUtils.getObject(bytes, clazz);
    }

    public static <T> T getInstance(byte[] bytes, int offset, int length, Class<T> clazz) {
        return SerdeUtils.getObject(bytes, offset, length, clazz);
    }
}
