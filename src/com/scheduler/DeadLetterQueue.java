package com.scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Dead Letter Queue that permanently stores jobs that have failed
 * after all retry attempts have been exhausted.
 */
public class DeadLetterQueue {
    private final BlockingQueue<Job> dlq = new LinkedBlockingQueue<>();

    public void add(Job job) {
        dlq.add(job);
    }

    public boolean isEmpty() {
        return dlq.isEmpty();
    }

    public int size() {
        return dlq.size();
    }

    public Job take() throws InterruptedException {
        return dlq.take();
    }

    public boolean contains(Job job) {
        return dlq.contains(job);
    }

    @Override
    public String toString() {
        return "DeadLetterQueue with " + size() + " failed job(s)";
    }
}
