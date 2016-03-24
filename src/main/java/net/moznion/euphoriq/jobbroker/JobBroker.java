package net.moznion.euphoriq.jobbroker;

import java.util.Optional;

import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.JobCanceledException;

public interface JobBroker {
    long enqueue(String queueName, Object arg);

    Optional<Job> dequeue() throws JobCanceledException;

    void cancel(long id);

    boolean registerRetryJob(final long id, final String queueName, final Object arg);

    void retry();
}
