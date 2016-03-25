package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.worker.JobWorker;

public interface JobWorkerFactory {
    JobWorker createWorker();
}
