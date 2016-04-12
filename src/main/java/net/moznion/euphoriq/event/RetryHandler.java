package net.moznion.euphoriq.event;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.jobbroker.JobFailedCountManager;
import net.moznion.euphoriq.jobbroker.RetryableJobBroker;
import net.moznion.euphoriq.jobbroker.Undertaker;
import net.moznion.euphoriq.worker.JobWorker;

public class RetryHandler<T extends JobBroker & RetryableJobBroker & JobFailedCountManager & Undertaker>
        implements EventHandler<T> {
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
        final long failedCount = jobBroker.incrementFailedCount(id);
        if (failedCount > 25) {
            jobBroker.sendToMorgue(id, queueName, argument, timeoutSec);
            return;
        }

        final double delay = Math.pow(failedCount, 4) + 15
                             + (new Random(Instant.now().getEpochSecond()).nextInt(30) * (failedCount + 1));

        jobBroker.registerRetryJob(id, queueName, argument, timeoutSec, delay);
    }
}
