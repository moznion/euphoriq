package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.SimpleJobWorker;
import net.moznion.euphoriq.worker.JobWorker;

public class SimpleJobWorkerFactory implements WorkerFactory<JobWorker> {
    private final JobBroker jobBroker;

    public SimpleJobWorkerFactory(final JobBroker jobBroker) {
        this.jobBroker = jobBroker;
    }

    @Override
    public JobWorker createWorker() {
        return new SimpleJobWorker(jobBroker);
    }
}
