package com.lightgraph.graph.utils;

import com.lightgraph.graph.exception.GraphException;
import java.util.concurrent.Callable;

public class RetryUtils {

    public static <V> V retry(Callable<V> callable, int count, int backoff) {
        int retry;
        for (retry = 0; retry < count; retry++) {
            try {
                return callable.call();
            } catch (Exception e) {
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
        throw new GraphException(String.format("max retry:%d failed!", count));
    }
}
