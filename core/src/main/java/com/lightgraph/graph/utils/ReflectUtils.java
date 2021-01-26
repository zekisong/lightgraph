package com.lightgraph.graph.utils;

public class ReflectUtils {

    public static <T> T getInstance(String classStr)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> clazz = Class.forName(classStr);
        T instance = (T) clazz.newInstance();
        return instance;
    }

}
