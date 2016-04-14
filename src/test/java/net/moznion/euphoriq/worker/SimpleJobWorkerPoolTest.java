package net.moznion.euphoriq.worker;

import static net.moznion.euphoriq.misc.Constant.NAMESPACE_FOR_TESTING;
import static net.moznion.euphoriq.misc.Constant.REDIS_HOST;
import static net.moznion.euphoriq.misc.Constant.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import net.moznion.euphoriq.event.Event;
import net.moznion.euphoriq.event.LoggingHandler;
import net.moznion.euphoriq.jobbroker.RedisJobBroker;
import net.moznion.euphoriq.misc.action.BarAction;
import net.moznion.euphoriq.misc.action.FooAction;
import net.moznion.euphoriq.misc.argument.BarArgument;
import net.moznion.euphoriq.misc.argument.FooArgument;
import net.moznion.euphoriq.worker.factory.SimpleJobWorkerFactory;

import redis.clients.jedis.Jedis;

public class SimpleJobWorkerPoolTest {
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

        final HashMap<String, Integer> weightedQueues = new HashMap<>();
        final String queueName = "normal-queue";
        weightedQueues.put(queueName, 1);

        final RedisJobBroker jobBroker = new RedisJobBroker(NAMESPACE_FOR_TESTING, weightedQueues,
                                                            REDIS_HOST, REDIS_PORT, 5);

        jobBroker.enqueue(queueName, new FooArgument("test1"));
        jobBroker.enqueue(queueName, new BarArgument(1234));
        jobBroker.enqueue(queueName, new FooArgument("test2"));
        jobBroker.enqueue(queueName, new BarArgument(5678));
        jobBroker.enqueue(queueName, new FooArgument("test3"));

        final SimpleJobWorkerPool<RedisJobBroker> workerPool =
                new SimpleJobWorkerPool<>(new SimpleJobWorkerFactory<>(jobBroker), 3, Optional.empty());
        workerPool.setActionMapping(FooArgument.class, FooAction.class);
        workerPool.setActionMapping(BarArgument.class, BarAction.class);

        workerPool.addEventHandler(Event.STARTED, new LoggingHandler<>());
        workerPool.addEventHandler(Event.FINISHED, new LoggingHandler<>());

        workerPool.run();
        Thread.sleep(1000);
        workerPool.shutdown(false);
        workerPool.join();

        try (final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test1")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test2")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "test3")).isEqualTo("foobar");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "1234")).isEqualTo("buzqux");
            assertThat(jedis.get(NAMESPACE_FOR_TESTING + "|XXX|" + "5678")).isEqualTo("buzqux");
        }
    }
}
