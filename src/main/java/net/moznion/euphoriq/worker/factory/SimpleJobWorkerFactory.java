package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.JobWorker;
import net.moznion.euphoriq.worker.SimpleJobWorker;

public class SimpleJobWorkerFactory<T extends JobBroker> implements WorkerFactory<JobWorker<T>> {
    private final T jobBroker;

    public SimpleJobWorkerFactory(final T jobBroker) {
        this.jobBroker = jobBroker;
    }

    @Override
    public JobWorker<T> createWorker() {
        return new SimpleJobWorker<T>(jobBroker);
    }
}
