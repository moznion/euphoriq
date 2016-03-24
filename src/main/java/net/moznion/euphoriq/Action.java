package net.moznion.euphoriq;

public interface Action<T> extends Runnable {
    void setArg(T arg);
}
