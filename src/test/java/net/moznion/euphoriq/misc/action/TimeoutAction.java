package net.moznion.euphoriq.misc.action;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.misc.argument.TimeoutArgument;

public class TimeoutAction implements Action<TimeoutArgument> {
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
