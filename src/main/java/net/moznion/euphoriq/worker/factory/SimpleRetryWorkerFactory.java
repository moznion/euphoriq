package net.moznion.euphoriq.worker.factory;

import java.util.OptionalInt;

import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.jobbroker.RetryableJobBroker;
import net.moznion.euphoriq.worker.SimpleRetryWorker;
import net.moznion.euphoriq.worker.Worker;

public class SimpleRetryWorkerFactory<T extends JobBroker & RetryableJobBroker> implements WorkerFactory<Worker> {
    private final T jobBroker;
    private final OptionalInt maybeIntervalMillis;

    public SimpleRetryWorkerFactory(final T jobBroker) {
        this.jobBroker = jobBroker;
        maybeIntervalMillis = OptionalInt.empty();
    }

    public SimpleRetryWorkerFactory(final T jobBroker, final int intervalMillis) {
        this.jobBroker = jobBroker;
        maybeIntervalMillis = OptionalInt.of(intervalMillis);
    }

    @Override
    public Worker createWorker() {
        if (maybeIntervalMillis.isPresent()) {
            return new SimpleRetryWorker<>(jobBroker, maybeIntervalMillis.getAsInt());
        }
        return new SimpleRetryWorker<>(jobBroker);
    }
}
