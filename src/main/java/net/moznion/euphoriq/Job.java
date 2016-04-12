package net.moznion.euphoriq;

import java.util.OptionalInt;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Job {
    private long id;
    private Object arg;
    private String queueName;
    private OptionalInt timeoutSec;
}
