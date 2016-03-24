package net.moznion.euphoriq.worker;

import net.moznion.euphoriq.Action;

public interface Worker extends Runnable {
    void join() throws InterruptedException;

    void shutdown(boolean immediately);

    // TODO rename
    <T> void setActionMapping(Class<T> argumentClass, Class<? extends Action<T>> actionClass);

    void addEventHandler(Event event, EventHandler handler);

    void setEventHandler(Event event, EventHandler handler);

    void clearEventHandler(Event event);
}
