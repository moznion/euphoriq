package net.moznion.euphoriq.misc.action;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.misc.Constant;
import net.moznion.euphoriq.misc.argument.BarArgument;

import redis.clients.jedis.Jedis;

public class BarAction implements Action<BarArgument> {
    private BarArgument barArgument;

    @Override
    public void setArg(BarArgument arg) {
        barArgument = arg;
    }

    @Override
    public void run() {
        try (final Jedis jedis = new Jedis(Constant.REDIS_HOST, Constant.REDIS_PORT)) {
            jedis.set(Constant.NAMESPACE_FOR_TESTING + "|XXX|" + barArgument.getNum(), "buzqux");
        }
    }
}
