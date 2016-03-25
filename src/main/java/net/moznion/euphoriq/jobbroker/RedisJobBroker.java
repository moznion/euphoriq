package net.moznion.euphoriq.jobbroker;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.JobCanceledException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RedisJobBroker implements JobBroker {
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
        if (!queuesBag.contains(queueName)) {
            // TODO
            throw new IllegalArgumentException();
        }

        try (final Jedis jedis = jedisPool.getResource()) {
            final Long id = jedis.incr(getIdPodKey());
            enqueue(jedis, new JobPayload(id, arg.getClass(), arg, queueName));
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

            final Job job = new Job(id, mapper.convertValue(jobPayload.arg, jobPayload.argumentClass), jobPayload.getQueueName());

            if (isCanceledJob(jedis, id)) {
                throw new JobCanceledException(job);
            }

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
    public void retry() {
        try (final Jedis jedis = jedisPool.getResource()) {
            final Set<String> serializedRetryJobPayloads = jedis.zrangeByScore(getFailedKey(), 0, Instant.now().getEpochSecond());
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
    public boolean registerRetryJob(final long id, final String queueName, final Object arg) {
        try (final Jedis jedis = jedisPool.getResource()) {
            long failedCount = 1;
            final String fetched = jedis.hget(getFailedCountKey(), String.valueOf(id));
            if (fetched != null) {
                failedCount = Long.parseLong(fetched, 10);
            }

            if (failedCount > 25) {
                // TODO go to morgue
                return false;
            }

            final double delay = Math.pow(failedCount, 4) + 15
                    + (new Random(Instant.now().getEpochSecond()).nextInt(30) * (failedCount + 1));

            final String serializedPayload = mapper.writeValueAsString(new JobPayload(id, arg.getClass(), arg, queueName));
            jedis.hincrBy(getFailedCountKey(), String.valueOf(id), 1);
            jedis.zadd(getFailedKey(), Instant.now().getEpochSecond() + delay, serializedPayload);
        } catch (JsonProcessingException e) {
            // TODO
            throw new RuntimeException();
        }
        return true;
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
        int currentCursor = cursor.get();

        for (int cnt = queuesSize; cnt > 0; cnt--) {
            final int index = currentCursor - 1;
            final String job = jedis.rpop(getQueueKey(queues.get(index))); // TODO: care runtime exception
            if (job != null) {
                incrementCursor(currentCursor);
                return Optional.of(job);
            }

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

    private String getFailedCountKey() {
        return namespace + "|failed_count";
    }

    private String getFailedKey() {
        return namespace + "|failed";
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class JobPayload {
        private long id;
        private Class<?> argumentClass;
        private Object arg;
        private String queueName;
    }
}
