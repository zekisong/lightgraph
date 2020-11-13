package com.lightgraph.graph.utils;

import com.lightgraph.graph.meta.DataType;

public class DataTypeUtils {

    public static <T> T getValue(String data, Class clazz) {
        T ret;
        if (clazz == String.class) {
            ret = (T) data;
        } else if (clazz == Integer.class) {
            ret = (T) Integer.valueOf(data);
        } else if (clazz == Long.class) {
            ret = (T) Long.valueOf(data);
        } else if (clazz == Short.class) {
            ret = (T) Short.valueOf(data);
        } else if (clazz == Float.class) {
            ret = (T) Float.valueOf(data);
        } else if (clazz == Double.class) {
            ret = (T) Double.valueOf(data);
        } else {
            throw new RuntimeException("not support this type " + clazz);
        }
        return ret;
    }

    public static <T> T getValue(String data, DataType dataType) {
        T ret;
        if (dataType == DataType.STRING) {
            ret = (T) data;
        } else if (dataType == DataType.INTEGER) {
            ret = (T) Integer.valueOf(data);
        } else if (dataType == DataType.LONG) {
            ret = (T) Long.valueOf(data);
        } else if (dataType == DataType.DOUBLE) {
            ret = (T) Double.valueOf(data);
        } else if (dataType == DataType.FLOAT) {
            ret = (T) Float.valueOf(data);
        } else if (dataType == DataType.TIMESTAMP) {
            ret = (T) Long.valueOf(data);
        } else {
            throw new RuntimeException("not support this type " + dataType);
        }
        return ret;
    }

    public static int sizeOf(DataType dataType) {
        if (dataType == DataType.STRING) {
            return 0;
        } else if (dataType == DataType.INTEGER) {
            return 4;
        } else if (dataType == DataType.LONG) {
            return 8;
        } else if (dataType == DataType.DOUBLE) {
            return 8;
        } else if (dataType == DataType.FLOAT) {
            return 4;
        } else if (dataType == DataType.TIMESTAMP) {
            return 8;
        } else {
            throw new RuntimeException("not support this type " + dataType);
        }
    }
}
