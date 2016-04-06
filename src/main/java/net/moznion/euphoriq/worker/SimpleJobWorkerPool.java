package net.moznion.euphoriq.worker;

import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.event.Event;
import net.moznion.euphoriq.event.EventHandler;
import net.moznion.euphoriq.jobbroker.JobBroker;
import net.moznion.euphoriq.worker.factory.WorkerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static net.moznion.euphoriq.event.Event.FAILED;

@Slf4j
public class SimpleJobWorkerPool<T extends JobBroker> implements JobWorker<T> {
    private final int workerNum;
    private final WorkerFactory<JobWorker<T>> jobWorkerFactory;
    private final ConcurrentHashMap<JobWorker<T>, Thread> workerThreadMap;
    private final ConcurrentHashMap<Event, List<EventHandler<T>>> eventHandlerMap;
    private final Optional<Pair<Worker, Thread>> retryWorkerThreadTuple;

    private final ThreadFactory threadFactory = Executors.defaultThreadFactory();
    private final EventHandler<T> workerNumAdjuster =
            (event, worker, jobBroker, clazz, id, arg, queueName, timeoutSec, throwable) -> adjustWorkerNum();

    public SimpleJobWorkerPool(final WorkerFactory<JobWorker<T>> jobWorkerFactory,
                               final int workerNum,
                               final Optional<WorkerFactory<Worker>> maybeRetryWorkerFactory) {
        this.workerNum = workerNum;
        this.jobWorkerFactory = jobWorkerFactory;
        workerThreadMap = new ConcurrentHashMap<>(workerNum);
        eventHandlerMap = initializeEventHandlerMap();
        retryWorkerThreadTuple = initializeRetryWorkerThreadTuple(maybeRetryWorkerFactory);

        for (int i = 0; i < workerNum; i++) {
            spawnWorker();
        }
    }

    @Override
    public <U> void setActionMapping(final Class<U> argumentClass,
                                     final Class<? extends Action<U>> actionClass) {
        Collections.list(workerThreadMap.keys())
                   .parallelStream()
                   .forEach(w -> w.setActionMapping(argumentClass, actionClass));
    }

    @Override
    public void run() {
        workerThreadMap.values().forEach(Thread::start);
        retryWorkerThreadTuple.ifPresent(pair -> pair.getValue().start());
        Thread.yield();
    }

    @Override
    public void join() throws InterruptedException {
        final Enumeration<JobWorker<T>> workers = workerThreadMap.keys();
        while (workers.hasMoreElements()) {
            workers.nextElement().join();
        }
        if (retryWorkerThreadTuple.isPresent()) {
            retryWorkerThreadTuple.get().getKey().join();
        }
    }

    @Override
    public void shutdown(final boolean immediately) {
        final Enumeration<JobWorker<T>> workers = workerThreadMap.keys();
        while (workers.hasMoreElements()) {
            workers.nextElement().shutdown(immediately);
        }
        if (retryWorkerThreadTuple.isPresent()) {
            retryWorkerThreadTuple.get().getKey().shutdown(immediately);
        }
    }

    @Override
    public void addEventHandler(final Event event, final EventHandler<T> handler) {
        eventHandlerMap.get(event).add(handler);

        Collections.list(workerThreadMap.keys())
                   .parallelStream()
                   .forEach(w -> w.addEventHandler(event, handler));
    }

    @Override
    public void setEventHandler(final Event event, final EventHandler<T> handler) {
        final List<EventHandler<T>> newHandlers =
                event == FAILED ? getInitialFailedEventHandlers() : new ArrayList<>();
        newHandlers.add(handler);
        eventHandlerMap.put(event, newHandlers);

        Collections.list(workerThreadMap.keys())
                   .parallelStream()
                   .forEach(w -> {
                       w.clearEventHandler(event);
                       newHandlers.forEach(h -> w.addEventHandler(event, h));
                   });
    }

    @Override
    public void clearEventHandler(final Event event) {
        final List<EventHandler<T>> newHandlers =
                event == FAILED ? getInitialFailedEventHandlers() : new ArrayList<>();
        eventHandlerMap.put(event, newHandlers);

        Collections.list(workerThreadMap.keys())
                   .parallelStream()
                   .forEach(w -> {
                       w.clearEventHandler(event);
                       newHandlers.forEach(h -> w.addEventHandler(event, h));
                   });
    }

    private void spawnWorker() {
        final JobWorker<T> worker = jobWorkerFactory.createWorker();

        for (Entry<Event, List<EventHandler<T>>> entry : eventHandlerMap.entrySet()) {
            final Event event = entry.getKey();
            final List<EventHandler<T>> eventHandlers = entry.getValue();
            eventHandlers.forEach(handler -> worker.addEventHandler(event, handler));
        }

        workerThreadMap.put(worker, threadFactory.newThread(worker));
    }

    private ConcurrentHashMap<Event, List<EventHandler<T>>> initializeEventHandlerMap() {
        final Event[] events = Event.values();
        final ConcurrentHashMap<Event, List<EventHandler<T>>> eventHandlerMap =
                new ConcurrentHashMap<>(events.length);

        for (final Event event : events) {
            eventHandlerMap.put(event, new ArrayList<>());
        }
        eventHandlerMap.put(FAILED, getInitialFailedEventHandlers());

        return eventHandlerMap;
    }

    private Optional<Pair<Worker, Thread>> initializeRetryWorkerThreadTuple(
            final Optional<WorkerFactory<Worker>> maybeRetryWorkerFactory) {
        if (maybeRetryWorkerFactory.isPresent()) {
            final Worker w = maybeRetryWorkerFactory.get().createWorker();
            Thread t = threadFactory.newThread(w);
            return Optional.of(new Pair<>(w, t));
        }
        return Optional.empty();
    }

    private List<EventHandler<T>> getInitialFailedEventHandlers() {
        final ArrayList<EventHandler<T>> initialFailedEventHandlers = new ArrayList<>();
        initialFailedEventHandlers.add(workerNumAdjuster);
        return initialFailedEventHandlers;
    }

    private void adjustWorkerNum() {
        int activeWorkersNum = 0;

        final Iterator<Thread> threads = workerThreadMap.values().iterator();
        if (threads.hasNext()) {
            final Thread thread = threads.next();
            if (thread.isAlive()) {
                activeWorkersNum++;
            }
        }

        removeExcessWorkers(activeWorkersNum);
        spawnLackedWorkers(activeWorkersNum);
    }

    private void removeExcessWorkers(final int activeWorkersNum) {
        if (activeWorkersNum <= workerNum) {
            return;
        }

        final int diff = activeWorkersNum - workerNum;
        final Enumeration<JobWorker<T>> workers = workerThreadMap.keys();

        for (int i = 0; i < diff; i++) {
            if (!workers.hasMoreElements()) {
                log.error("Failed to remove an excess worker");
                break;
            }

            final JobWorker<T> worker = workers.nextElement();
            worker.shutdown(false);
            try {
                worker.join();
            } catch (InterruptedException ignore) {
                log.error("Failed to join an excess worker");
                break;
            }
            workerThreadMap.remove(worker);
        }
    }

    private void spawnLackedWorkers(final int activeWorkersNum) {
        if (activeWorkersNum >= workerNum) {
            return;
        }

        final int diff = workerNum - activeWorkersNum;
        for (int i = 0; i < diff; i++) {
            spawnWorker();
        }
    }
}
