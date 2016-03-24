package net.moznion.euphoriq.worker;

import static net.moznion.euphoriq.worker.Event.FAILED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import net.moznion.euphoriq.Action;
import net.moznion.euphoriq.worker.factory.WorkerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleWorkerPool implements Worker {
    private final WorkerFactory workerFactory;
    private final ThreadFactory threadFactory;
    private final ConcurrentHashMap<Worker, Thread> workerThreadMap;
    private final int workerNum;
    private final ConcurrentHashMap<Event, List<EventHandler>> eventHandlerMap;
    private final EventHandler workerNumAdjuster;

    public SimpleWorkerPool(final WorkerFactory workerFactory, final int workerNum) {
        this.workerNum = workerNum;
        this.workerFactory = workerFactory;
        threadFactory = Executors.defaultThreadFactory();
        workerThreadMap = new ConcurrentHashMap<>(workerNum);

        workerNumAdjuster = (worker, clazz, id, arg, throwable) -> adjustWorkerNum();
        eventHandlerMap = initializeEventHandlerMap();

        for (int i = 0; i < workerNum; i++) {
            spawnWorker();
        }
    }

    @Override
    public <T> void setActionMapping(final Class<T> argumentClass,
                                     final Class<? extends Action<T>> actionClass) {
        Collections.list(workerThreadMap.keys())
                   .parallelStream()
                   .forEach(w -> w.setActionMapping(argumentClass, actionClass));
    }

    @Override
    public void run() {
        workerThreadMap.values().forEach(Thread::start);
        Thread.yield();
    }

    @Override
    public void join() throws InterruptedException {
        final Enumeration<Worker> workers = workerThreadMap.keys();
        while (workers.hasMoreElements()) {
            workers.nextElement().join();
        }
    }

    @Override
    public void shutdown(final boolean immediately) {
        final Enumeration<Worker> workers = workerThreadMap.keys();
        while (workers.hasMoreElements()) {
            workers.nextElement().shutdown(immediately);
        }
    }

    @Override
    public void addEventHandler(final Event event, final EventHandler handler) {
        eventHandlerMap.get(event).add(handler);

        Collections.list(workerThreadMap.keys())
                   .parallelStream()
                   .forEach(w -> w.addEventHandler(event, handler));
    }

    @Override
    public void setEventHandler(final Event event, final EventHandler handler) {
        final List<EventHandler> newHandlers =
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
        final List<EventHandler> newHandlers =
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
        final Worker worker = workerFactory.createWorker();

        for (Entry<Event, List<EventHandler>> entry : eventHandlerMap.entrySet()) {
            final Event event = entry.getKey();
            final List<EventHandler> eventHandlers = entry.getValue();
            eventHandlers.forEach(handler -> worker.addEventHandler(event, handler));
        }

        workerThreadMap.put(worker, threadFactory.newThread(worker));
    }

    private ConcurrentHashMap<Event, List<EventHandler>> initializeEventHandlerMap() {
        final Event[] events = Event.values();
        final ConcurrentHashMap<Event, List<EventHandler>> eventHandlerMap =
                new ConcurrentHashMap<>(events.length);

        for (final Event event : events) {
            eventHandlerMap.put(event, new ArrayList<>());
        }
        eventHandlerMap.put(FAILED, getInitialFailedEventHandlers());

        return eventHandlerMap;
    }

    private List<EventHandler> getInitialFailedEventHandlers() {
        final ArrayList<EventHandler> initialFailedEventHandlers = new ArrayList<>();
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
        final Enumeration<Worker> workers = workerThreadMap.keys();

        for (int i = 0; i < diff; i++) {
            if (!workers.hasMoreElements()) {
                log.error("Failed to remove an excess worker");
                break;
            }

            final Worker worker = workers.nextElement();
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
