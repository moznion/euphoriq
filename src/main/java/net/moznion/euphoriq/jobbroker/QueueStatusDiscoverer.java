package net.moznion.euphoriq.jobbroker;

public interface QueueStatusDiscoverer {
    long getNumberOfWaitingJobs();

    long getNumberOfProcessedJobs();

    long getNumberOfRetryWaitingJobs();
}
