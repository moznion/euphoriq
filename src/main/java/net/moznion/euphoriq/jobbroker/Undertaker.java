package net.moznion.euphoriq.jobbroker;

import java.util.List;
import java.util.OptionalInt;

import net.moznion.euphoriq.Job;

public interface Undertaker {
    List<Job> getAllDiedJobs();

    List<Job> getDiedJobs(long start, long end);

    long getNumberOfDiedJobs();

    void sendToMorgue(long id, String queueName, Object argument, OptionalInt timeoutSec);
}
