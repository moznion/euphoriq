package net.moznion.euphoriq.worker;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.jobbroker.JobBroker;

import java.util.Optional;

@FunctionalInterface
public interface EventHandler {
    void handle(Worker worker,
                JobBroker jobBroker,
                Optional<Class<? extends Action<?>>> actionClass,
                long id,
                Object argument,
                String queueName,
                Optional<Throwable> throwable);
}
