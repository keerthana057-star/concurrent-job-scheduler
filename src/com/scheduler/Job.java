package com.scheduler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a single unit of work submitted to the scheduler.
 *
 * A Job is Comparable so it can live inside a PriorityBlockingQueue:
 *  1. Higher priority wins.
 *  2. For equal priority, the lower sequence number wins (FIFO).
 */
public class Job implements Comparable<Job> {

    private final String id;
    private final String name;
    private final JobPriority priority;
    private final Runnable task;
    private final long sequence;      // insertion order, used for FIFO tie-break
    private final int maxRetries;

    private volatile JobStatus status;
    private final AtomicInteger attempts = new AtomicInteger(0);

    public Job(String id, String name, JobPriority priority, Runnable task,
               long sequence, int maxRetries) {
        this.id = id;
        this.name = name;
        this.priority = priority;
        this.task = task;
        this.sequence = sequence;
        this.maxRetries = maxRetries;
        this.status = JobStatus.PENDING;
    }

    // --- Getters ---
    public String getId()        { return id; }
    public String getName()      { return name; }
    public JobPriority getPriority() { return priority; }
    public Runnable getTask()    { return task; }
    public long getSequence()    { return sequence; }
    public int getMaxRetries()   { return maxRetries; }
    public JobStatus getStatus() { return status; }

    // --- Status & attempts (thread-safe) ---
    public void setStatus(JobStatus status) {
        this.status = status;
    }

    /** Returns how many times this job has been started. */
    public int incrementAttempts() {
        return attempts.incrementAndGet();
    }

    public int getAttempts() {
        return attempts.get();
    }

    /**
     * Ordering for the priority queue.
     * Higher priority first; equal priority -> earlier submitted first.
     */
    @Override
    public int compareTo(Job other) {
        int byPriority = Integer.compare(
                other.priority.getLevel(), this.priority.getLevel());
        if (byPriority != 0) {
            return byPriority;
        }
        return Long.compare(this.sequence, other.sequence);
    }

    @Override
    public String toString() {
        return "Job{id=" + id + ", name=" + name + ", priority=" + priority +
               ", status=" + status + ", attempts=" + attempts.get() + "}";
    }
}
