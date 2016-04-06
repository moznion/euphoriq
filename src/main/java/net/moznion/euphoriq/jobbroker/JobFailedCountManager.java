package net.moznion.euphoriq.jobbroker;

public interface JobFailedCountManager {
    long incrementFailedCount(long id);

    long getFailedCount(long id);
}
