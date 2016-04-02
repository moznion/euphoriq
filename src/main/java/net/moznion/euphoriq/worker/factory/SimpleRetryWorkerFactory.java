package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.SimpleRetryWorker;
import net.moznion.euphoriq.worker.Worker;

import java.util.OptionalInt;

public class SimpleRetryWorkerFactory implements WorkerFactory<Worker> {
    private final JobBroker jobBroker;
    private final OptionalInt maybeIntervalMillis;

    public SimpleRetryWorkerFactory(final JobBroker jobBroker) {
        this.jobBroker = jobBroker;
        this.maybeIntervalMillis = OptionalInt.empty();
    }

    public SimpleRetryWorkerFactory(final JobBroker jobBroker, final int intervalMillis) {
        this.jobBroker = jobBroker;
        this.maybeIntervalMillis = OptionalInt.of(intervalMillis);
    }

    @Override
    public Worker createWorker() {
        if (maybeIntervalMillis.isPresent()) {
            return new SimpleRetryWorker(jobBroker, maybeIntervalMillis.getAsInt());
        }
        return new SimpleRetryWorker(jobBroker);
    }
}
