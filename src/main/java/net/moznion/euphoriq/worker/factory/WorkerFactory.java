package net.moznion.euphoriq.worker.factory;

import net.moznion.euphoriq.worker.Worker;

public interface WorkerFactory {
    Worker createWorker();
}
