package net.moznion.euphoriq.worker;

import lombok.extern.slf4j.Slf4j;
import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.event.Event;
import net.moznion.euphoriq.event.EventHandler;
import net.moznion.euphoriq.exception.ActionNotFoundException;
import net.moznion.euphoriq.exception.JobCanceledException;
import net.moznion.euphoriq.jobbroker.JobBroker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static net.moznion.euphoriq.event.Event.CANCELED;
import static net.moznion.euphoriq.event.Event.ERROR;
import static net.moznion.euphoriq.event.Event.FAILED;
import static net.moznion.euphoriq.event.Event.FINISHED;
import static net.moznion.euphoriq.event.Event.STARTED;
import static net.moznion.euphoriq.event.Event.TIMEOUT;

@Slf4j
public class SimpleJobWorker implements JobWorker {
    private final JobBroker jobBroker;
    private final ConcurrentHashMap<Class<?>, Class<? extends Action<?>>> actionMap;
    private final AtomicReference<Thread> threadRef;
    private final ConcurrentHashMap<Event, List<EventHandler>> eventHandlerMap;

    private boolean isShuttingDown;

    public SimpleJobWorker(final JobBroker jobBroker) {
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
            final Optional<Job> maybeJob;
            try {
                maybeJob = jobBroker.dequeue();
            } catch (JobCanceledException e) {
                final Job job = e.getJob();
                final Object arg = job.getArg();
                final Class<? extends Action<?>> actionClass = actionMap.get(arg.getClass());
                handleCanceledEvent(actionClass, job.getId(), arg, job.getQueueName());
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

            final Job job = maybeJob.get();
            final long id = job.getId();
            final Object arg = job.getArg();
            final String queueName = job.getQueueName();
            final Class<? extends Action<?>> actionClass = actionMap.get(arg.getClass());
            if (actionClass == null) {
                handleErrorEvent(actionClass, id, arg, queueName, new ActionNotFoundException());
                continue;
            }

            final Action action;
            try {
                action = actionClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                handleErrorEvent(actionClass, id, arg, queueName, e);
                continue;
            }
            action.setArg(arg);

            final ExecutorService executorService = Executors.newSingleThreadExecutor();

            handleStartedEvent(actionClass, id, arg, queueName);

            final Future<?> future = executorService.submit(action);
            try {
                final OptionalInt maybeTimeoutSec = job.getTimeoutSec();
                if (maybeTimeoutSec.isPresent()) {
                    future.get(maybeTimeoutSec.getAsInt(), TimeUnit.SECONDS);
                } else {
                    future.get();
                }
            } catch (InterruptedException e) {
                handleFailedEvent(actionClass, id, arg, queueName, e);
                continue;
            } catch (ExecutionException e) {
                handleFailedEvent(actionClass, id, arg, queueName, e.getCause());
                continue;
            } catch (TimeoutException e) {
                handleTimeoutEvent(actionClass, id, arg, queueName, e);
                continue;
            } finally {
                executorService.shutdown();
            }

            handleFinishedEvent(actionClass, id, arg, queueName);
        }
    }

    private void handleStartedEvent(final Class<? extends Action<?>> actionClass,
                                    final long id,
                                    final Object arg,
                                    final String queueName) {
        eventHandlerMap.get(STARTED).forEach(h -> h.handle(this,
                                                           jobBroker,
                                                           Optional.ofNullable(actionClass),
                                                           id,
                                                           arg,
                                                           queueName,
                                                           Optional.empty()));
    }

    private void handleFailedEvent(final Class<? extends Action<?>> actionClass,
                                   final long id,
                                   final Object arg,
                                   final String queueName,
                                   final Throwable e) {
        eventHandlerMap.get(FAILED).forEach(h -> h.handle(this,
                                                          jobBroker,
                                                          Optional.ofNullable(actionClass),
                                                          id,
                                                          arg,
                                                          queueName,
                                                          Optional.of(e)));
    }

    private void handleFinishedEvent(final Class<? extends Action<?>> actionClass,
                                     final long id,
                                     final Object arg,
                                     final String queueName) {
        eventHandlerMap.get(FINISHED).forEach(h -> h.handle(this,
                                                            jobBroker,
                                                            Optional.ofNullable(actionClass),
                                                            id,
                                                            arg,
                                                            queueName,
                                                            Optional.empty()));
    }

    private void handleCanceledEvent(final Class<? extends Action<?>> actionClass,
                                     final long id,
                                     final Object arg,
                                     final String queueName) {
        eventHandlerMap.get(CANCELED).forEach(h -> h.handle(this,
                                                            jobBroker,
                                                            Optional.ofNullable(actionClass),
                                                            id,
                                                            arg,
                                                            queueName,
                                                            Optional.empty()));
    }

    private void handleErrorEvent(final Class<? extends Action<?>> actionClass,
                                  final long id,
                                  final Object arg,
                                  final String queueName,
                                  final Throwable e) {
        eventHandlerMap.get(ERROR).forEach(h -> h.handle(this,
                                                         jobBroker,
                                                         Optional.ofNullable(actionClass),
                                                         id,
                                                         arg,
                                                         queueName,
                                                         Optional.of(e)));
    }

    private void handleTimeoutEvent(final Class<? extends Action<?>> actionClass,
                                  final long id,
                                  final Object arg,
                                  final String queueName,
                                  final Throwable e) {
        eventHandlerMap.get(TIMEOUT).forEach(h -> h.handle(this,
                                                           jobBroker,
                                                           Optional.ofNullable(actionClass),
                                                           id,
                                                           arg,
                                                           queueName,
                                                           Optional.of(e)));
    }
}
