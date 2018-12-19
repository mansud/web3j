package org.web3j.backport.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// TODO[CompletableFuture]: Provide a suitable replacement for this class.
public class CompletableFuture<V> implements Future<V> {

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
    public V get() throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }

    public void completeExceptionally(Exception e) {
    }

    public void complete(Object reply) {
    }

    public CompletableFuture<V> thenAccept(Runnable runnable) {
        return this;
    }
}
