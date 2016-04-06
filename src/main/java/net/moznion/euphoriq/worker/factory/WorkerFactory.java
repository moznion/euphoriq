package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.worker.Worker;

@FunctionalInterface
public interface WorkerFactory<T extends Worker> {
    T createWorker();
}
