package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.worker.JobWorker;

public interface WorkerFactory {
    JobWorker createWorker();
}
