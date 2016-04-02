package net.moznion.euphoriq.event;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.JobWorker;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;

public class RetryHandler implements EventHandler {
    @Override
    public void handle(JobWorker worker,
                       JobBroker jobBroker,
                       Optional<Class<? extends Action<?>>> actionClass,
                       long id,
                       Object argument,
                       String queueName,
                       OptionalInt timeoutSec,
                       Optional<Throwable> throwable) {
        final long failedCount = jobBroker.incrementFailedCount(id);
        if (failedCount > 25) {
            // TODO go to morgue
            return;
        }

        final double delay = Math.pow(failedCount, 4) + 15
                + (new Random(Instant.now().getEpochSecond()).nextInt(30) * (failedCount + 1));

        jobBroker.registerRetryJob(id, queueName, argument, timeoutSec, delay);
    }
}
