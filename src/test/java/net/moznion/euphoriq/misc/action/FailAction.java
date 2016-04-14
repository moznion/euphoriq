package net.moznion.euphoriq.misc.action;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.misc.argument.FailArgument;

public class FailAction implements Action<FailArgument> {
    @Override
    public void setArg(FailArgument arg) {
    }

    @Override
    public void run() {
        throw new RuntimeException();
    }
}
