package util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureTimeLimitGetter {
    private static final boolean DEBUG = false;
    private FutureTimeLimitGetter() {
    }

    public static <T> T getFutureResultOrDefault(Future<T> future, T defaultRespWhenError, long readTimeout) {
        try {
            return future.get(readTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return defaultRespWhenError;
        } catch (ExecutionException | TimeoutException e) {
            if (DEBUG) {
                printUsefulInfo(e);
            }
            return defaultRespWhenError;
        }
    }

    public static <T> List<T> generateFutureResultListOrDefault(List<Future<T>> input, T defaultValue, long restWaitingTime) {
        List<T> result = new ArrayList<>();
        for (Future<T> future : input) {
            try {
                long startTime = System.currentTimeMillis();
                result.add(future.get(restWaitingTime, TimeUnit.MILLISECONDS));
                restWaitingTime -= (System.currentTimeMillis() - startTime);
                restWaitingTime = Math.max(0, restWaitingTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (DEBUG) {
                    printUsefulInfo(e);
                }
                result.add(defaultValue);
            } catch (Exception e) {
                if (DEBUG) {
                    printUsefulInfo(e);
                }
                result.add(defaultValue);
            }
        }
        return result;
    }

    private static void printUsefulInfo(Exception e) {
        if (!(e instanceof InterruptedException) && !(e instanceof TimeoutException))
            System.err.println(e.getMessage());
    }
}
