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
}
