package net.moznion.euphoriq.worker;

import java.util.Optional;

import net.moznion.euphoriq.Action;

@FunctionalInterface
public interface EventHandler {
    void handle(Worker worker,
                Optional<Class<? extends Action<?>>> actionClass,
                Object argument,
                Optional<Throwable> throwable);
}
