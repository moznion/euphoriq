package net.moznion.euphoriq.worker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.jobbroker.RedisJobBroker;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleJobWorkerTest {
    private static final String REDIS_HOST = "127.0.0.1";
    private static final int REDIS_PORT = 6379;
    private static final String NAMESPACE_FOR_TESTING = "euphoriq-test";

    @Before
    public void setup() throws Exception {
        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            jedis.keys(NAMESPACE_FOR_TESTING + "*").forEach(jedis::del);
        }
    }

    @Test
    public void testBasic() throws Exception {
        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "test1")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "test2")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "test3")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "1234")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "5678")).isNull();
        }

        final String queueName = "normal-queue";

        final HashMap<String, Integer> weightedQueues = new HashMap<>();
        weightedQueues.put(queueName, 1);

        final RedisJobBroker jobBroker =
                new RedisJobBroker(NAMESPACE_FOR_TESTING, weightedQueues, REDIS_HOST, REDIS_PORT, 10);
        jobBroker.enqueue(queueName, new FooArgument("test1"));
        jobBroker.enqueue(queueName, new BarArgument(1234));
        jobBroker.enqueue(queueName, new FooArgument("test2"));
        jobBroker.enqueue(queueName, new BarArgument(5678));
        jobBroker.enqueue(queueName, new FooArgument("test3"));

        final SimpleJobWorker<RedisJobBroker> worker = new SimpleJobWorker<>(jobBroker);
        worker.setActionMapping(FooArgument.class, FooAction.class);
        worker.setActionMapping(BarArgument.class, BarAction.class);

        worker.start();

        Thread.sleep(1000);

        worker.shutdown(false);
        worker.join();

        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "test1")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "test2")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "test3")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "1234")).isEqualTo("buzqux");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|" + "5678")).isEqualTo("buzqux");
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FooArgument {
        private String label;
    }

    public static class FooAction implements Action<FooArgument> {
        private FooArgument fooArgument;

        @Override
        public void run() {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|" + fooArgument.getLabel(), "foobar");
            }
        }

        @Override
        public void setArg(FooArgument arg) {
            this.fooArgument = arg;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class BarArgument {
        private int num;
    }

    public static class BarAction implements Action<BarArgument> {
        private BarArgument barArgument;

        @Override
        public void setArg(BarArgument arg) {
            this.barArgument = arg;
        }

        @Override
        public void run() {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|" + barArgument.getNum(), "buzqux");
            }
        }
    }
}
