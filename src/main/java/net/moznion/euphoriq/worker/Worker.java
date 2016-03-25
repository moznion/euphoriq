package net.moznion.euphoriq.worker;

public interface Worker extends Runnable {
    void join() throws InterruptedException;

    void shutdown(boolean immediately);
}
