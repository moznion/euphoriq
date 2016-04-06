package net.moznion.euphoriq.jobbroker;

import java.util.OptionalInt;

public interface RetryableJobBroker {
    void retry();

    boolean registerRetryJob(long id,
                             String queueName,
                             Object arg,
                             OptionalInt timeoutSec,
                             double delay);
}
