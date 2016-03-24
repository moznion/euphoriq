package net.moznion.euphoriq.jobbroker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.JobCanceledException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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
                          final int redisPort) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(50); // TODO: configurable

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
            jedis.lpush(getQueueKey(queueName),
                        mapper.writeValueAsString(new Payload(id, arg.getClass(), arg)));
            return id;
        } catch (JsonProcessingException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Job> dequeue() throws JobCanceledException {
        try (final Jedis jedis = jedisPool.getResource()) {
            final Optional<String> maybeJobString = pickupJobString(jedis);
            if (!maybeJobString.isPresent()) {
                return Optional.empty();
            }

            final Payload payload = mapper.readValue(maybeJobString.get(), Payload.class);
            final long id = payload.getId();

            final Job job = new Job(id, mapper.convertValue(payload.arg, payload.argumentClass));

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

    private Optional<String> pickupJobString(final Jedis jedis) {
        int currentCursor = cursor.get();

        for (int cnt = queuesSize; cnt > 0; cnt--) {
            final int index = currentCursor - 1;
            log.info("Q: {}", queues.get(index));
            String job = jedis.rpop(getQueueKey(queues.get(index))); // TODO: care runtime exception
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

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Payload {
        private long id;
        private Class<?> argumentClass;
        private Object arg;
    }
}
