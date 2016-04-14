package net.moznion.euphoriq.worker;

import static net.moznion.euphoriq.event.Event.CANCELED;
import static net.moznion.euphoriq.event.Event.ERROR;
import static net.moznion.euphoriq.event.Event.FAILED;
import static net.moznion.euphoriq.event.Event.FINISHED;
import static net.moznion.euphoriq.event.Event.STARTED;
import static net.moznion.euphoriq.event.Event.TIMEOUT;

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

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.Job;
import net.moznion.euphoriq.event.Event;
import net.moznion.euphoriq.event.EventHandler;
import net.moznion.euphoriq.exception.ActionNotFoundException;
import net.moznion.euphoriq.exception.JobCanceledException;
import net.moznion.euphoriq.jobbroker.JobBroker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleJobWorker<T extends JobBroker> implements JobWorker<T> {
    private final T jobBroker;
    private final ConcurrentHashMap<Class<?>, Class<? extends Action<?>>> actionMap;
    private final AtomicReference<Thread> threadRef;
    private final ConcurrentHashMap<Event, List<EventHandler<T>>> eventHandlerMap;

    private boolean isShuttingDown;

    public SimpleJobWorker(final T jobBroker) {
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
    public <U> void setActionMapping(Class<U> argumentClass, Class<? extends Action<U>> actionClass) {
        actionMap.put(argumentClass, actionClass);
    }

    @Override
    public void run() {
        final Thread thread = Thread.currentThread();
        threadRef.set(thread);
        log.info("Worker started [threadId={}]", thread.getId());
        poll();
    }

    public void start() {
        final Thread thread = Executors.defaultThreadFactory().newThread(this);
        threadRef.set(thread);
        thread.start();
    }

    @Override
    public void join() throws InterruptedException {
        final Thread thread = threadRef.get();
        if (thread == null) {
            log.info("No such worker for joining");
        } else if (!thread.isAlive()) {
            log.info("Worker already finished [threadId={}]", thread.getId());
        } else {
            log.info("Waiting for joining [threadId={}]", thread.getId());
            thread.join();
            log.info("Worker joined [threadId={}]", thread.getId());
        }
    }

    @Override
    public void shutdown(final boolean immediately) {
        final Thread thread = threadRef.get();
        log.info("Worker attempts to shutdown [threadId={}, immediately={}]", thread.getId(), immediately);
        isShuttingDown = true;
        if (immediately) {
            if (thread != null && thread.isAlive()) {
                threadRef.get().interrupt();
            }
        }
    }

    @Override
    public void addEventHandler(final Event event, final EventHandler<T> handler) {
        eventHandlerMap.get(event).add(handler);
    }

    @Override
    public void setEventHandler(final Event event, final EventHandler<T> handler) {
        final ArrayList<EventHandler<T>> newHandlers = new ArrayList<>();
        newHandlers.add(handler);
        eventHandlerMap.put(event, newHandlers);
    }

    @Override
    public void clearEventHandler(final Event event) {
        eventHandlerMap.put(event, new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    private void poll() {
        while (!isShuttingDown) {
            final Optional<Job> maybeJob;
            try {
                maybeJob = jobBroker.dequeue();
            } catch (JobCanceledException e) {
                final Job job = e.getJob();
                final Object arg = job.getArg();
                final Class<? extends Action<?>> actionClass = actionMap.get(arg.getClass());
                handleCanceledEvent(actionClass, job.getId(), arg, job.getQueueName(), job.getTimeoutSec());
                continue;
            }

            if (!maybeJob.isPresent()) {
                // queue is empty
                try {
                    Thread.sleep(1000); // TODO sleep time should be configurable?
                } catch (InterruptedException e) {
                    log.warn("Interrupted in sleep");
                }
                continue;
            }

            final Job job = maybeJob.get();
            final long id = job.getId();
            final Object arg = job.getArg();
            final String queueName = job.getQueueName();
            final OptionalInt maybeTimeoutSec = job.getTimeoutSec();
            final Class<? extends Action<?>> actionClass = actionMap.get(arg.getClass());
            if (actionClass == null) {
                handleErrorEvent(null, id, arg, queueName, maybeTimeoutSec, new ActionNotFoundException());
                continue;
            }

            final Action action;
            try {
                action = actionClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                handleErrorEvent(actionClass, id, arg, queueName, maybeTimeoutSec, e);
                continue;
            }
            action.setArg(arg);

            final ExecutorService executorService = Executors.newSingleThreadExecutor();

            handleStartedEvent(actionClass, id, arg, queueName, maybeTimeoutSec);

            final Future<?> future = executorService.submit(action);
            try {
                if (maybeTimeoutSec.isPresent()) {
                    future.get(maybeTimeoutSec.getAsInt(), TimeUnit.SECONDS);
                } else {
                    future.get();
                }
            } catch (InterruptedException e) {
                handleFailedEvent(actionClass, id, arg, queueName, maybeTimeoutSec, e);
                continue;
            } catch (ExecutionException e) {
                handleFailedEvent(actionClass, id, arg, queueName, maybeTimeoutSec, e.getCause());
                continue;
            } catch (TimeoutException e) {
                handleTimeoutEvent(actionClass, id, arg, queueName, maybeTimeoutSec, e);
                continue;
            } finally {
                executorService.shutdown();
            }

            handleFinishedEvent(actionClass, id, arg, queueName, maybeTimeoutSec);
        }

        log.info("Worker shutdown[threadId={}]", threadRef.get().getId());
    }

    private void handleStartedEvent(final Class<? extends Action<?>> actionClass,
                                    final long id,
                                    final Object arg,
                                    final String queueName,
                                    final OptionalInt timeoutSec) {
        eventHandlerMap.get(STARTED).forEach(h -> h.handle(STARTED,
                                                           this,
                                                           jobBroker,
                                                           Optional.ofNullable(actionClass),
                                                           id,
                                                           arg,
                                                           queueName,
                                                           timeoutSec,
                                                           Optional.empty()));
    }

    private void handleFailedEvent(final Class<? extends Action<?>> actionClass,
                                   final long id,
                                   final Object arg,
                                   final String queueName,
                                   final OptionalInt timeoutSec,
                                   final Throwable e) {
        eventHandlerMap.get(FAILED).forEach(h -> h.handle(FAILED,
                                                          this,
                                                          jobBroker,
                                                          Optional.ofNullable(actionClass),
                                                          id,
                                                          arg,
                                                          queueName,
                                                          timeoutSec,
                                                          Optional.of(e)));
    }

    private void handleFinishedEvent(final Class<? extends Action<?>> actionClass,
                                     final long id,
                                     final Object arg,
                                     final String queueName,
                                     final OptionalInt timeoutSec) {
        eventHandlerMap.get(FINISHED).forEach(h -> h.handle(FINISHED,
                                                            this,
                                                            jobBroker,
                                                            Optional.ofNullable(actionClass),
                                                            id,
                                                            arg,
                                                            queueName,
                                                            timeoutSec,
                                                            Optional.empty()));
    }

    private void handleCanceledEvent(final Class<? extends Action<?>> actionClass,
                                     final long id,
                                     final Object arg,
                                     final String queueName,
                                     final OptionalInt timeoutSec) {
        eventHandlerMap.get(CANCELED).forEach(h -> h.handle(CANCELED,
                                                            this,
                                                            jobBroker,
                                                            Optional.ofNullable(actionClass),
                                                            id,
                                                            arg,
                                                            queueName,
                                                            timeoutSec,
                                                            Optional.empty()));
    }

    private void handleErrorEvent(final Class<? extends Action<?>> actionClass,
                                  final long id,
                                  final Object arg,
                                  final String queueName,
                                  final OptionalInt timeoutSec,
                                  final Throwable e) {
        eventHandlerMap.get(ERROR).forEach(h -> h.handle(ERROR,
                                                         this,
                                                         jobBroker,
                                                         Optional.ofNullable(actionClass),
                                                         id,
                                                         arg,
                                                         queueName,
                                                         timeoutSec,
                                                         Optional.of(e)));
    }

    private void handleTimeoutEvent(final Class<? extends Action<?>> actionClass,
                                    final long id,
                                    final Object arg,
                                    final String queueName,
                                    final OptionalInt timeoutSec,
                                    final Throwable e) {
        eventHandlerMap.get(TIMEOUT).forEach(h -> h.handle(TIMEOUT,
                                                           this,
                                                           jobBroker,
                                                           Optional.ofNullable(actionClass),
                                                           id,
                                                           arg,
                                                           queueName,
                                                           timeoutSec,
                                                           Optional.of(e)));
    }
}
