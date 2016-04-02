package net.moznion.euphoriq.jobbroker;

import java.util.Optional;
import java.util.OptionalInt;

import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.JobCanceledException;

public interface JobBroker {
    long enqueue(String queueName, Object arg);

    long enqueue(String queueName, Object arg, int timeoutSec);

    Optional<Job> dequeue() throws JobCanceledException;

    void cancel(long id);

    long incrementFailedCount(long id);

    long getFailedCount(long id);

    void retry();

    boolean registerRetryJob(final long id,
                             final String queueName,
                             final Object arg,
                             final OptionalInt timeoutSec,
                             final double delay);
}
