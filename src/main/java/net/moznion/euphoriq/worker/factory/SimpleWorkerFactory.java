package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.SimpleWorker;
import net.moznion.euphoriq.worker.Worker;

public class SimpleWorkerFactory implements WorkerFactory {
    private final JobBroker jobBroker;

    public SimpleWorkerFactory(final JobBroker jobBroker) {
        this.jobBroker = jobBroker;
    }

    @Override
    public Worker createWorker() {
        return new SimpleWorker(jobBroker);
    }
}
