package net.moznion.euphoriq.worker;

import net.moznion.euphoriq.Action;

import java.util.Optional;

@FunctionalInterface
public interface EventHandler {
    void handle(Worker worker,
                Optional<Class<? extends Action<?>>> actionClass,
                long id,
                Object argument,
                Optional<Throwable> throwable);
}
