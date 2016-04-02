package net.moznion.euphoriq.event;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.JobWorker;

import java.util.Optional;
import java.util.OptionalInt;

@FunctionalInterface
public interface EventHandler {
    void handle(JobWorker worker,
                JobBroker jobBroker,
                Optional<Class<? extends Action<?>>> actionClass,
                long id,
                Object argument,
                String queueName,
                OptionalInt timeoutSec,
                Optional<Throwable> throwable);
}
