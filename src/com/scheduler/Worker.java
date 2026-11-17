package com.scheduler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker continuously polls the JobQueue and executes jobs.
 * On failure, it delegates retry scheduling to the RetryHandler
 * so that worker threads never sleep.
 *
 * The worker polls with a timeout so it can exit cleanly when
 * shutdown is requested and the queue is empty.
 */
public class Worker implements Runnable {
    private final JobQueue jobQueue;
    private final RetryHandler retryHandler;
    private final long pollTimeoutMillis;
    private final AtomicBoolean shutdownFlag;

    public Worker(JobQueue jobQueue, RetryHandler retryHandler, AtomicBoolean shutdownFlag) {
        this(jobQueue, retryHandler, shutdownFlag, 500L);
    }

    public Worker(JobQueue jobQueue, RetryHandler retryHandler,
                  AtomicBoolean shutdownFlag, long pollTimeoutMillis) {
        this.jobQueue = jobQueue;
        this.retryHandler = retryHandler;
        this.pollTimeoutMillis = pollTimeoutMillis;
        this.shutdownFlag = shutdownFlag;
    }

    @Override
    public void run() {
        while (!shutdownFlag.get() || !jobQueue.isEmpty()) {
            try {
                Job job = jobQueue.poll(pollTimeoutMillis, TimeUnit.MILLISECONDS);
                if (job == null) {
                    continue;
                }
                processJob(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("[Worker] interrupted, exiting");
                break;
            } catch (Exception e) {
                System.out.println("[Worker] unexpected error: " + e.getMessage());
            }
        }
    }

    private void processJob(Job job) {
        int attempt = job.incrementAttempts();
        job.setStatus(JobStatus.RUNNING);
        System.out.println("[Worker] Executing " + job);
        try {
            job.getTask().run();
            job.setStatus(JobStatus.COMPLETED);
            System.out.println("[Worker] Completed " + job);
        } catch (Exception ex) {
            job.setStatus(JobStatus.FAILED);
            System.out.println("[Worker] Failed " + job + " - " + ex.getMessage());
            retryHandler.schedule(job, attempt + 1);
        }
    }
}