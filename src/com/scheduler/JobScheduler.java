package com.scheduler;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JobScheduler is the orchestrator that manages the job queue,
 * worker pool, retry handler, and dead letter queue.
 *
 * It provides methods to submit jobs, query status, and perform
 * graceful shutdown.
 */
public class JobScheduler {
    private final JobQueue jobQueue;
    private final DeadLetterQueue deadLetterQueue;
    private final RetryHandler retryHandler;
    private final ExecutorService workerPool;
    private final ScheduledExecutorService retryScheduler;
    private final Map<String, Job> jobRegistry = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);
    private volatile boolean acceptingJobs = true;
    private final int defaultMaxRetries;
    private final long baseBackoffMillis;

    private static final int DEFAULT_NUM_WORKERS = 4;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_BACKOFF_MILLIS = 1000L;

    public JobScheduler() {
        this(DEFAULT_NUM_WORKERS, DEFAULT_MAX_RETRIES, DEFAULT_BACKOFF_MILLIS);
    }

    public JobScheduler(int numWorkers, int maxRetries, long baseBackoffMillis) {
        this.defaultMaxRetries = maxRetries;
        this.baseBackoffMillis = baseBackoffMillis;

        this.jobQueue = new JobQueue();
        this.deadLetterQueue = new DeadLetterQueue();
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor();
        this.retryHandler = new RetryHandler(jobQueue, deadLetterQueue, retryScheduler, baseBackoffMillis);
        this.workerPool = Executors.newFixedThreadPool(numWorkers);

        // start worker threads
        for (int i = 0; i < numWorkers; i++) {
            Worker worker = new Worker(jobQueue, retryHandler, shutdownFlag);
            workerPool.submit(worker);
        }
        System.out.println("[JobScheduler] Started with " + numWorkers + " worker threads");
    }

    /**
     * Submit a job to the scheduler. The job is added to the priority queue
     * and also tracked in the registry for status queries.
     */
    public String submitJob(String name, JobPriority priority, Runnable task) {
        return submitJob(name, priority, task, defaultMaxRetries);
    }

    /**
     * Submit a job with a custom maximum retry count.
     */
    public String submitJob(String name, JobPriority priority, Runnable task, int maxRetries) {
        if (!acceptingJobs) {
            throw new IllegalStateException("Scheduler is shutting down; cannot submit new jobs.");
        }
        long sequence = jobQueue.nextSequence();
        String jobId = "job-" + sequence + "-" + System.nanoTime();
        Job job = new Job(jobId, name, priority, task, sequence, maxRetries);
        jobRegistry.put(jobId, job);
        jobQueue.submit(job);
        System.out.println("[JobScheduler] Submitted " + job);
        return jobId;
    }

    /**
     * Get the current status of a job by its ID.
     */
    public JobStatus getJobStatus(String jobId) {
        Job job = jobRegistry.get(jobId);
        return job != null ? job.getStatus() : null;
    }

    /**
     * Get the job object for inspection (e.g., attempt count).
     */
    public Job getJob(String jobId) {
        return jobRegistry.get(jobId);
    }

    /**
     * Get the dead letter queue for inspection.
     */
    public DeadLetterQueue getDeadLetterQueue() {
        return deadLetterQueue;
    }

    /**
     * Check if the scheduler is still accepting new jobs.
     */
    public boolean isAcceptingJobs() {
        return acceptingJobs;
    }

    /**
     * Check if the worker pool has terminated.
     */
    public boolean isTerminated() {
        return workerPool.isTerminated();
    }

    /**
     * Gracefully shut down the scheduler:
     * 1. Stop accepting new jobs.
     * 2. Signal workers to stop polling.
     * 3. Allow running jobs to finish and drain remaining queue.
     * 4. Shut down the worker pool and retry scheduler.
     */
    public void shutdown() {
        acceptingJobs = false;
        System.out.println("[JobScheduler] Shutdown initiated; no new jobs will be accepted.");

        // Signal workers to stop (they will drain the queue first)
        shutdownFlag.set(true);

        try {
            workerPool.shutdown();
            if (!workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out.println("[JobScheduler] Worker pool did not terminate gracefully; forcing shutdown.");
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workerPool.shutdownNow();
        }

        // Shut down retry scheduler
        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            retryScheduler.shutdownNow();
        }

        // Log any jobs still in the dead letter queue
        if (!deadLetterQueue.isEmpty()) {
            System.out.println("[JobScheduler] Dead letter queue contains " + deadLetterQueue.size() + " permanently failed job(s).");
        }

        System.out.println("[JobScheduler] Shutdown complete.");
    }
}