package server.model;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimeoutFuture<T> implements Future<T> {

    @Override
    public T get(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
        assert unit == TimeUnit.MILLISECONDS;
        Thread.sleep(timeout);
        throw new TimeoutException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return null;
    }
}
