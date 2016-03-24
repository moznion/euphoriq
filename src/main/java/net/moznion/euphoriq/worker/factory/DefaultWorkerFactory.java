package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.DefaultWorker;
import net.moznion.euphoriq.worker.Worker;

public class DefaultWorkerFactory implements WorkerFactory {
    private final JobBroker jobBroker;

    public DefaultWorkerFactory(final JobBroker jobBroker) {
        this.jobBroker = jobBroker;
    }

    @Override
    public Worker createWorker() {
        return new DefaultWorker(jobBroker);
    }
}
