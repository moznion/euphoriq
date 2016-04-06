package net.moznion.euphoriq.jobbroker;

public interface FailedCountManager {
    long incrementFailedCount(long id);

    long getFailedCount(long id);
}
