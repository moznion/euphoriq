package net.moznion.euphoriq.jobbroker;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.JobCanceledException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
public class RedisJobBroker
        implements JobBroker, RetryableJobBroker, JobFailedCountManager, QueueStatusDiscoverer, Undertaker {
    private static final int CURSOR_INITIAL_VALUE = 1;

    private final JedisPool jedisPool;
    private final ObjectMapper mapper;
    private final AtomicInteger cursor;
    private final List<String> queues;
    private final Set<String> queuesBag;
    private final int queuesSize;
    private final String namespace;

    public RedisJobBroker(final String namespace,
                          final Map<String, Integer> queuesWithWeight,
                          final String redisHost,
                          final int redisPort,
                          final int redisConnectionNum) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(redisConnectionNum);

        jedisPool = new JedisPool(poolConfig, redisHost, redisPort); // TODO: timeout, password

        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        this.namespace = namespace;

        queues = initializeQueues(queuesWithWeight);
        queuesSize = queues.size();
        if (queuesSize <= 0) {
            throw new IllegalArgumentException("TODO");
        }
        queuesBag = queuesWithWeight.keySet();

        cursor = new AtomicInteger(CURSOR_INITIAL_VALUE);
    }

    @Override
    public long enqueue(String queueName, Object arg) {
        return enqueue(queueName, arg, OptionalInt.empty());
    }

    @Override
    public long enqueue(String queueName, Object arg, int timeoutSec) {
        return enqueue(queueName, arg, OptionalInt.of(timeoutSec));
    }

    private long enqueue(String queueName, Object arg, OptionalInt timeoutSecOptional) {
        if (!queuesBag.contains(queueName)) {
            // TODO
            throw new IllegalArgumentException();
        }

        try (final Jedis jedis = jedisPool.getResource()) {
            final Long id = jedis.incr(getIdPodKey());
            enqueue(jedis, new JobPayload(id, arg.getClass(), arg, queueName, timeoutSecOptional));
            return id;
        }
    }

    @Override
    public Optional<Job> dequeue() throws JobCanceledException {
        try (final Jedis jedis = jedisPool.getResource()) {
            final Optional<String> maybeSerializedJobPayload = pickupSerializedPayload(jedis);
            if (!maybeSerializedJobPayload.isPresent()) {
                return Optional.empty();
            }

            final JobPayload jobPayload = mapper.readValue(maybeSerializedJobPayload.get(), JobPayload.class);
            final long id = jobPayload.getId();

            final Job job = new Job(id,
                                    mapper.convertValue(jobPayload.arg, jobPayload.argumentClass),
                                    jobPayload.getQueueName(),
                                    jobPayload.getTimeoutSec());

            if (isCanceledJob(jedis, id)) {
                throw new JobCanceledException(job);
            }

            jedis.incr(getProcessedCountKey());

            return Optional.of(job);
        } catch (JsonMappingException e) {
            // TODO
            throw new RuntimeException(e);
        } catch (JsonParseException e) {
            // TODO
            throw new RuntimeException(e);
        } catch (IOException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cancel(final long id) {
        try (final Jedis jedis = jedisPool.getResource()) {
            jedis.hset(getCanceledJobKey(), String.valueOf(id), "1");
        }
    }

    @Override
    public long incrementFailedCount(final long id) {
        try (final Jedis jedis = jedisPool.getResource()) {
            return jedis.incr(getFailedCountKey(id));
        }
    }

    @Override
    public long getFailedCount(final long id) {
        try (final Jedis jedis = jedisPool.getResource()) {
            final String fetched = jedis.get(getFailedCountKey(id));
            if (fetched == null) {
                return 0L;
            }
            return Long.parseLong(fetched, 10);
        }
    }

    @Override
    public void retry() {
        try (final Jedis jedis = jedisPool.getResource()) {
            final long epochSecond = Instant.now().getEpochSecond();
            final String failedKey = getFailedKey();

            final Set<String> serializedRetryJobPayloads = jedis.zrangeByScore(failedKey, 0, epochSecond);
            jedis.zremrangeByScore(failedKey, 0, epochSecond);
            for (final String serializedRetryJobPayload : serializedRetryJobPayloads) {
                try {
                    final JobPayload jobPayload = mapper.readValue(serializedRetryJobPayload, JobPayload.class);
                    enqueue(jedis, jobPayload);
                } catch (IOException e) {
                    // TODO
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public boolean registerRetryJob(final long id,
                                    final String queueName,
                                    final Object arg,
                                    final OptionalInt timeoutSec,
                                    final double delay) {
        try (final Jedis jedis = jedisPool.getResource()) {
            final JobPayload jobPayload = new JobPayload(id, arg.getClass(), arg, queueName, timeoutSec);
            final String serializedPayload = mapper.writeValueAsString(jobPayload);
            jedis.zadd(getFailedKey(), Instant.now().getEpochSecond() + delay, serializedPayload);
        } catch (JsonProcessingException e) {
            // TODO
            throw new RuntimeException();
        }
        return true;
    }

    @Override
    public long getNumberOfWaitingJobs() {
        long num = 0;
        try (final Jedis jedis = jedisPool.getResource()) {
            for (final String queue : queuesBag) {
                final Long len = jedis.llen(getQueueKey(queue));
                if (len != null) {
                    num += len;
                }
            }
        }
        return num;
    }

    @Override
    public long getNumberOfProcessedJobs() {
        try (final Jedis jedis = jedisPool.getResource()) {
            return Long.valueOf(jedis.get(getProcessedCountKey()), 10);
        }
    }

    @Override
    public long getNumberOfRetryWaitingJobs() {
        try (final Jedis jedis = jedisPool.getResource()) {
            return jedis.zcard(getFailedKey());
        }
    }

    @Override
    public List<Job> getAllDiedJobs() {
        return getDiedJobs(0, -1);
    }

    @Override
    public List<Job> getDiedJobs(long start, long end) {
        final List<String> items;
        try (final Jedis jedis = jedisPool.getResource()) {
            items = jedis.lrange(getMorgueKey(), start, end);
        }

        final List<Job> jobs = new ArrayList<>();
        for (final String item : items) {
            try {
                jobs.add(mapper.readValue(item, Job.class));
            } catch (IOException e) {
                // TODO
                e.printStackTrace();
            }
        }

        return jobs;
    }

    @Override
    public long getNumberOfDiedJobs() {
        try (final Jedis jedis = jedisPool.getResource()) {
            return jedis.llen(getMorgueKey());
        }
    }

    @Override
    public void sendToMorgue(long id, String queueName, Object argument, OptionalInt timeoutSec) {
        try (final Jedis jedis = jedisPool.getResource()) {
            jedis.rpush(mapper.writeValueAsString(new Job(id, argument, queueName, timeoutSec)));
        } catch (JsonProcessingException e) {
            // TODO
            e.printStackTrace();
        }
    }

    private void enqueue(final Jedis jedis, final JobPayload jobPayload) {
        final String serializedRetryJobPayload;
        try {
            serializedRetryJobPayload = mapper.writeValueAsString(jobPayload);
            jedis.lpush(getQueueKey(jobPayload.getQueueName()), serializedRetryJobPayload);
        } catch (JsonProcessingException e) {
            /// TODO
            e.printStackTrace();
        }
    }

    private Optional<String> pickupSerializedPayload(final Jedis jedis) {
        final Set<String> visitedQueues = new HashSet<>();

        int currentCursor = cursor.get();

        for (int cnt = queuesSize; cnt > 0; cnt--) {
            final int index = currentCursor - 1;
            final String queue = queues.get(index);

            if (!visitedQueues.contains(queue)) {
                // Visiting queue for the first time
                final String job = jedis.rpop(getQueueKey(queue)); // TODO: care runtime exception
                if (job != null) {
                    incrementCursor(currentCursor);
                    return Optional.of(job);
                }

                // queue is empty
                visitedQueues.add(queue);
            }

            // queue is empty or already visited
            if (currentCursor % queuesSize == 0) {
                currentCursor = 1;
            } else {
                currentCursor++;
            }
        }

        // Scan all of queues and they are empty
        incrementCursor(currentCursor);
        return Optional.empty();
    }

    private void incrementCursor(final int i) {
        if (i % queuesSize == 0) {
            cursor.set(1);
        } else {
            cursor.set(i + 1);
        }
    }

    private static List<String> initializeQueues(final Map<String, Integer> queueWithWeight) {
        final ArrayList<String> queues = new ArrayList<>();
        for (Entry<String, Integer> entry : queueWithWeight.entrySet()) {
            final String queueName = entry.getKey();
            final Integer weight = entry.getValue();
            for (Integer i = 0; i < weight; i++) {
                queues.add(queueName);
            }
        }
        Collections.shuffle(queues);
        return queues;
    }

    private boolean isCanceledJob(final Jedis jedis, final long id) {
        return jedis.hdel(getCanceledJobKey(), String.valueOf(id)) != 0;
    }

    private String getQueueKey(final String queueName) {
        return namespace + "|queue|" + queueName;
    }

    private String getIdPodKey() {
        return namespace + "|id";
    }

    private String getCanceledJobKey() {
        return namespace + "|canceled";
    }

    private String getFailedCountKey(final long id) {
        return namespace + "|failed_count|" + id;
    }

    private String getFailedKey() {
        return namespace + "|failed";
    }

    private String getProcessedCountKey() {
        return namespace + "|processed_count";
    }

    private String getMorgueKey() {
        return namespace + "|morgue";
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class JobPayload {
        private long id;
        private Class<?> argumentClass;
        private Object arg;
        private String queueName;
        private OptionalInt timeoutSec;
    }
}
