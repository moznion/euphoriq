package net.moznion.euphoriq;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Job {
    private long id;
    private Object arg;
    private String queueName;
}
