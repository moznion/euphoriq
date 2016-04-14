package net.moznion.euphoriq.misc.action;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.misc.Constant;
import net.moznion.euphoriq.misc.argument.FooArgument;

import redis.clients.jedis.Jedis;

public class FooAction implements Action<FooArgument> {
    private FooArgument fooArgument;

    @Override
    public void run() {
        try (final Jedis jedis = new Jedis(Constant.REDIS_HOST, Constant.REDIS_PORT)) {
            jedis.set(Constant.NAMESPACE_FOR_TESTING + "|XXX|" + fooArgument.getLabel(), "foobar");
        }
    }

    @Override
    public void setArg(FooArgument arg) {
        fooArgument = arg;
    }
}
