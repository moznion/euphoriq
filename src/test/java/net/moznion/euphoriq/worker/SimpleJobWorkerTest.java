package net.moznion.euphoriq.worker;

import static net.moznion.euphoriq.misc.Constant.NAMESPACE_FOR_TESTING;
import static net.moznion.euphoriq.misc.Constant.REDIS_HOST;
import static net.moznion.euphoriq.misc.Constant.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import net.moznion.euphoriq.event.Event;
import net.moznion.euphoriq.jobbroker.RedisJobBroker;
import net.moznion.euphoriq.misc.action.BarAction;
import net.moznion.euphoriq.misc.action.FailAction;
import net.moznion.euphoriq.misc.action.FooAction;
import net.moznion.euphoriq.misc.action.TimeoutAction;
import net.moznion.euphoriq.misc.argument.BarArgument;
import net.moznion.euphoriq.misc.argument.FailArgument;
import net.moznion.euphoriq.misc.argument.FooArgument;
import net.moznion.euphoriq.misc.argument.TimeoutArgument;
import net.moznion.euphoriq.worker.factory.SimpleJobWorkerFactory;

import redis.clients.jedis.Jedis;

public class SimpleJobWorkerTest {
    @Before
    public void setup() throws Exception {
        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            jedis.keys(NAMESPACE_FOR_TESTING + '*').forEach(jedis::del);
        }
    }

    @Test
    public void testBasic() throws Exception {
        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test1")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test2")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test3")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "1234")).isNull();
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "5678")).isNull();
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

        final SimpleJobWorker<RedisJobBroker> worker = new SimpleJobWorkerFactory<>(jobBroker).createWorker();
        worker.setActionMapping(FooArgument.class, FooAction.class);
        worker.setActionMapping(BarArgument.class, BarAction.class);

        worker.start();

        Thread.sleep(1000);

        worker.shutdown(false);
        worker.join();

        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test1")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test2")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test3")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "1234")).isEqualTo("buzqux");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "5678")).isEqualTo("buzqux");
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

        final SimpleJobWorker<RedisJobBroker> worker = new SimpleJobWorkerFactory<>(jobBroker).createWorker();
        worker.setActionMapping(FooArgument.class, FooAction.class);
        worker.setActionMapping(FailArgument.class, FailAction.class);
        worker.setActionMapping(TimeoutArgument.class, TimeoutAction.class);
        worker.addEventHandler(Event.STARTED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|XXX|started|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.FINISHED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|XXX|finished|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.CANCELED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|XXX|canceled|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.FAILED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|XXX|failed|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.ERROR, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|XXX|error|" + id, String.valueOf(id));
            }
        });
        worker.addEventHandler(Event.TIMEOUT, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|XXX|timeout|" + id, String.valueOf(id));
            }
        });

        worker.start();

        Thread.sleep(5000);

        worker.shutdown(false);
        worker.join();

        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|started|" + id1)).isEqualTo(String.valueOf(id1));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|finished|" + id1)).isEqualTo(String.valueOf(id1));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|canceled|" + cancelId)).isEqualTo(String.valueOf(cancelId));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|failed|" + failedId)).isEqualTo(String.valueOf(failedId));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|error|" + errorId)).isEqualTo(String.valueOf(errorId));
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|timeout|" + timeoutId)).isEqualTo(String.valueOf(timeoutId));
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

        final SimpleJobWorker<RedisJobBroker> worker = new SimpleJobWorkerFactory<>(jobBroker).createWorker();
        worker.setActionMapping(FooArgument.class, FooAction.class);
        worker.addEventHandler(Event.STARTED, (event, w, jb, actionClass, id, argument, qn, timeoutSec, throwable) -> {
            try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
                jedis.set(NAMESPACE_FOR_TESTING + "|XXX|started|" + id, String.valueOf(id));
            }
        });
        worker.clearEventHandler(Event.STARTED);

        worker.start();

        Thread.sleep(1000);

        worker.shutdown(false);
        worker.join();

        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|started|" + id1)).isNull();
        }
    }

}
