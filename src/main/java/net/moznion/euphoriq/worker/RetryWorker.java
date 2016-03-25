package net.moznion.euphoriq.worker;

public interface RetryWorker {
    void join() throws InterruptedException;

    void shutdown(boolean immediately);

    void poll();
}
