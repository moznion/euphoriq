package net.moznion.euphoriq.worker;

import static net.moznion.euphoriq.worker.Event.CANCELED;
import static net.moznion.euphoriq.worker.Event.ERROR;
import static net.moznion.euphoriq.worker.Event.FAILED;
import static net.moznion.euphoriq.worker.Event.FINISHED;
import static net.moznion.euphoriq.worker.Event.STARTED;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.exception.ActionNotFoundException;
import net.moznion.euphoriq.exception.JobCanceledException;
import net.moznion.euphoriq.jobbroker.JobBroker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultWorker implements Worker {
    private final JobBroker jobBroker;
    private final ConcurrentHashMap<Class<?>, Class<? extends Action<?>>> actionMap;
    private final AtomicReference<Thread> threadRef;
    private final ConcurrentHashMap<Event, List<EventHandler>> eventHandlerMap;

    private boolean isShuttingDown;

    public DefaultWorker(final JobBroker jobBroker) {
        this.jobBroker = jobBroker;

        actionMap = new ConcurrentHashMap<>();
        threadRef = new AtomicReference<>(null);
        isShuttingDown = false;

        final Event[] events = Event.values();
        eventHandlerMap = new ConcurrentHashMap<>(events.length);
        for (final Event event : events) {
            eventHandlerMap.put(event, new ArrayList<>());
        }
    }

    @Override
    public <T> void setActionMapping(final Class<T> argumentClass,
                                     final Class<? extends Action<T>> actionClass) {
        actionMap.put(argumentClass, actionClass);
    }

    @Override
    public void run() {
        threadRef.set(Thread.currentThread());
        poll();
    }

    @Override
    public void join() throws InterruptedException {
        final Thread thread = threadRef.get();
        if (thread != null && thread.isAlive()) {
            thread.join();
        }
    }

    @Override
    public void shutdown(final boolean immediately) {
        isShuttingDown = true;
        if (immediately) {
            final Thread thread = threadRef.get();
            if (thread != null && thread.isAlive()) {
                threadRef.get().interrupt();
            }
        }
    }

    @Override
    public void addEventHandler(final Event event, final EventHandler handler) {
        eventHandlerMap.get(event).add(handler);
    }

    @Override
    public void setEventHandler(final Event event, final EventHandler handler) {
        final ArrayList<EventHandler> newHandlers = new ArrayList<>();
        newHandlers.add(handler);
        eventHandlerMap.put(event, newHandlers);
    }

    @Override
    public void clearEventHandler(final Event event) {
        eventHandlerMap.put(event, new ArrayList<>());
    }

    private void poll() {
        while (!isShuttingDown) {
            Optional<Job> maybeJob;
            try {
                maybeJob = jobBroker.dequeue();
            } catch (JobCanceledException e) {
                final Object arg = e.getJob().getArg();
                final Class<? extends Action<?>> actionClass = actionMap.get(arg.getClass());
                handleCanceledEvent(actionClass, arg);
                continue;
            }

            if (!maybeJob.isPresent()) {
                // queue is empty
                try {
                    Thread.sleep(1000); // TODO sleep time should be configurable?
                } catch (InterruptedException e) {
                    // TODO more suitable error handling
                    log.error("Failed to sleep");
                }
                continue;
            }

            final Object arg = maybeJob.get().getArg();
            final Class<? extends Action<?>> actionClass = actionMap.get(arg.getClass());
            if (actionClass == null) {
                handleErrorEvent(actionClass, arg, new ActionNotFoundException());
                continue;
            }

            final Action action;
            try {
                action = actionClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                handleErrorEvent(actionClass, arg, e);
                continue;
            }
            action.setArg(arg);

            handleStartedEvent(actionClass, arg);

            try {
                action.run();
            } catch (RuntimeException e) {
                handleFailedEvent(actionClass, arg, e);
                continue;
            }

            handleFinishedEvent(actionClass, arg);
        }
    }

    private void handleStartedEvent(final Class<? extends Action<?>> actionClass, final Object arg) {
        eventHandlerMap.get(STARTED).forEach(h -> h.handle(this,
                                                           Optional.of(actionClass),
                                                           arg,
                                                           Optional.empty()));
    }

    private void handleFailedEvent(final Class<? extends Action<?>> actionClass,
                                   final Object arg,
                                   final RuntimeException e) {
        eventHandlerMap.get(FAILED).forEach(h -> h.handle(this,
                                                          Optional.of(actionClass),
                                                          arg,
                                                          Optional.of(e)));
    }

    private void handleFinishedEvent(final Class<? extends Action<?>> actionClass, final Object arg) {
        eventHandlerMap.get(FINISHED).forEach(h -> h.handle(this,
                                                            Optional.of(actionClass),
                                                            arg,
                                                            Optional.empty()));
    }

    private void handleCanceledEvent(final Class<? extends Action<?>> actionClass, final Object arg) {
        eventHandlerMap.get(CANCELED).forEach(h -> h.handle(this,
                                                            Optional.ofNullable(actionClass),
                                                            arg,
                                                            Optional.empty()));
    }

    private void handleErrorEvent(final Class<? extends Action<?>> actionClass,
                                  final Object arg,
                                  final Exception e) {
        eventHandlerMap.get(ERROR).forEach(h -> h.handle(this,
                                                         Optional.ofNullable(actionClass),
                                                         arg,
                                                         Optional.of(e)));
    }
}
