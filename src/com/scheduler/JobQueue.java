package com.scheduler;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread-safe priority queue of jobs.
 *
 * PriorityBlockingQueue is unbounded and blocking. Jobs are ordered by
 * their natural ordering (priority, then FIFO sequence number), so the
 * worker threads always pull the highest-priority job that is ready.
 */
public class JobQueue {
    private final PriorityBlockingQueue<Job> queue = new PriorityBlockingQueue<>();
    private final AtomicInteger sequence = new AtomicInteger(0);

    /** Adds a job to the queue (never blocks because the queue is unbounded). */
    public void submit(Job job) {
        queue.put(job);
    }

    /** Blocks until a job is available (used by workers). */
    public Job take() throws InterruptedException {
        return queue.take();
    }

    /** Waits up to the given timeout for a job. Returns null on timeout. */
    public Job poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    /** Returns the next insertion sequence number (used for FIFO tie-breaks). */
    public long nextSequence() {
        return sequence.getAndIncrement();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }
}
