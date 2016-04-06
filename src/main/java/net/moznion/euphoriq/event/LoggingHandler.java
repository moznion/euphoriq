package net.moznion.euphoriq.event;

import java.util.Optional;
import java.util.OptionalInt;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.JobWorker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggingHandler<T extends JobBroker> implements EventHandler<T> {
    @Override
    public void handle(Event event,
                       JobWorker<T> worker,
                       T jobBroker,
                       Optional<Class<? extends Action<?>>> actionClass,
                       long id,
                       Object argument,
                       String queueName,
                       OptionalInt timeoutSec,
                       Optional<Throwable> throwable) {
        final long threadId = Thread.currentThread().getId();
        if (throwable.isPresent()) {
            log.info("{}: threadId={}, queueName={}, actionClass={}, jobId={}, jobArgument={}",
                     event.name(), threadId, queueName, actionClass.orElse(null), id, argument, throwable);
        } else {
            log.info("{}: threadId={}, queueName={}, actionClass={}, jobId={}, jobArgument={}",
                     event.name(), threadId, queueName, actionClass.orElse(null), id, argument);
        }
    }
}
