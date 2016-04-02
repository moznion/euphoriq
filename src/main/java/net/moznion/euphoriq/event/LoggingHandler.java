package net.moznion.euphoriq.event;

import lombok.extern.slf4j.Slf4j;
import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.JobWorker;

import java.util.Optional;
import java.util.OptionalInt;

@Slf4j
public class LoggingHandler implements EventHandler {
    @Override
    public void handle(Event event,
                       JobWorker worker,
                       JobBroker jobBroker,
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
