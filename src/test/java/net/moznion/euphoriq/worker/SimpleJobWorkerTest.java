package net.moznion.euphoriq.worker;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.event.Event;
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

    @Test
    public void testEventHandler() throws Exception {
        final String queueName = "normal-queue";
        final HashMap<String, Integer> weightedQueues = new HashMap<>();
        weightedQueues.put(queueName, 1);

        final RedisJobBroker jobBroker =
                new RedisJobBroker(NAMESPACE_FOR_TESTING, weightedQueues, REDIS_HOST, REDIS_PORT, 10);
        final long id1 = jobBroker.enqueue(queueName, new FooArgument("test1"));
        final long failedId = jobBroker.enqueue(queueName, new FailArgument());
        final long cancelId = jobBroker.enqueue(queueName, new FooArgument("cancel"));
        final long errorId = jobBroker.enqueue(queueName, new Exception());
        final long timeoutId = jobBroker.enqueue(queueName, new TimeoutArgument(), 1);
        jobBroker.cancel(cancelId);

        final SimpleJobWorker<RedisJobBroker> worker = new SimpleJobWorker<>(jobBroker);
        worker.setActionMapping(FooArgument.class, FooAction.class);
        worker.setActionMapping(FailArgument.class, FailAction.class);
        worker.setActionMapping(TimeoutArgument.class, TimeoutAction.class);
        worker.addEventHandler(Event.STARTED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|started|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.FINISHED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|finished|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.CANCELED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|canceled|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.FAILED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|failed|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.ERROR, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|error|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.TIMEOUT, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|timeout|" + id, String.valueOf(id));
            }
        });

        worker.start();

        Thread.sleep(5000);

        worker.shutdown(false);
        worker.join();

        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|started|" + id1)).isEqualTo(String.valueOf(id1));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|finished|" + id1)).isEqualTo(String.valueOf(id1));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|canceled|" + cancelId)).isEqualTo(String.valueOf(cancelId));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|failed|" + failedId)).isEqualTo(String.valueOf(failedId));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|error|" + errorId)).isEqualTo(String.valueOf(errorId));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|timeout|" + timeoutId)).isEqualTo(String.valueOf(timeoutId));
        }
    }

    @Test
    public void testClearEventHandler() throws Exception {
        final String queueName = "normal-queue";
        final HashMap<String, Integer> weightedQueues = new HashMap<>();
        weightedQueues.put(queueName, 1);

        final RedisJobBroker jobBroker =
                new RedisJobBroker(NAMESPACE_FOR_TESTING, weightedQueues, REDIS_HOST, REDIS_PORT, 10);
        final long id1 = jobBroker.enqueue(queueName, new FooArgument("test1"));

        final SimpleJobWorker<RedisJobBroker> worker = new SimpleJobWorker<>(jobBroker);
        worker.setActionMapping(FooArgument.class, FooAction.class);
        worker.addEventHandler(Event.STARTED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|started|" + id, String.valueOf(id));
            }
        });
        worker.clearEventHandler(Event.STARTED);

        worker.start();

        Thread.sleep(1000);

        worker.shutdown(false);
        worker.join();

        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|started|" + id1)).isNull();
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

    @Data
    @JsonAutoDetect(fieldVisibility = Visibility.ANY)
    public static class FailArgument {
    }

    public static class FailAction implements Action<FailArgument> {
        @Override
        public void setArg(FailArgument arg) {
        }

        @Override
        public void run() {
            throw new RuntimeException();
        }
    }

    @Data
    @JsonAutoDetect(fieldVisibility = Visibility.ANY)
    public static class TimeoutArgument {
    }

    public static class TimeoutAction implements Action<TimeoutArgument> {
        @Override
        public void setArg(TimeoutArgument arg) {
        }

        @Override
        public void run() {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
        }
    }
}
