package net.moznion.euphoriq.worker;

import java.util.concurrent.atomic.AtomicReference;

import net.moznion.euphoriq.jobbroker.JobBroker;

public class SimpleRetryWorker implements Worker {
    private static final int DEFAULT_INTERVAL_MILLIS = 10000; // 10 sec

    private final JobBroker jobBroker;
    private final int intervalMillis;
    private final AtomicReference<Thread> threadRef;
    private boolean isShuttingDown;

    public SimpleRetryWorker(final JobBroker jobBroker) {
        this(jobBroker, DEFAULT_INTERVAL_MILLIS);
    }

    public SimpleRetryWorker(final JobBroker jobBroker, final int intervalMillis) {
        this.jobBroker = jobBroker;
        this.intervalMillis = intervalMillis;
        isShuttingDown = false;
        threadRef = new AtomicReference<>(null);
    }

    @Override
    public void run() {
        threadRef.set(Thread.currentThread());
        poll();
    }

    private void poll() {
        while (!isShuttingDown) {
            jobBroker.retry();
            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                // TODO log?
                continue;
            }
        }
    }

    @Override
    public void join() throws InterruptedException {
        final Thread thread = threadRef.get();
        if (thread != null && thread.isAlive()) {
            thread.join();
        }
    }

    @Override
    public void shutdown(boolean immediately) {
        isShuttingDown = true;
        if (immediately) {
            final Thread thread = threadRef.get();
            if (thread != null && thread.isAlive()) {
                threadRef.get().interrupt();
            }
        }
    }
}
