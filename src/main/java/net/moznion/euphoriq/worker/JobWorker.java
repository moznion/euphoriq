package net.moznion.euphoriq.worker;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.event.Event;
import net.moznion.euphoriq.event.EventHandler;
import net.moznion.euphoriq.jobbroker.JobBroker;

public interface JobWorker<T extends JobBroker> extends Worker {
    // TODO rename
    <U> void setActionMapping(Class<U> argumentClass, Class<? extends Action<U>> actionClass);

    void addEventHandler(Event event, EventHandler<T> handler);

    void setEventHandler(Event event, EventHandler<T> handler);

    void clearEventHandler(Event event);
}
