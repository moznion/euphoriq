package net.moznion.euphoriq.exception;

import net.moznion.euphoriq.Job;

import lombok.Getter;

public class JobCanceledException extends Exception {
    private static final long serialVersionUID = 9074092794930152740L;

    @Getter
    private final Job job;

    public JobCanceledException(final Job job) {
        this.job = job;
    }
}
