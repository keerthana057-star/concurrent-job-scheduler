package com.scheduler;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Handles retry scheduling for failed jobs with exponential backoff.
 *
 * On failure, if a job hasn't reached its max retries, schedule a re-enqueue
 * after the computed backoff delay. If max retries reached, delegate to DLQ.
 */
public class RetryHandler {
    private final ScheduledExecutorService scheduler;
    private final JobQueue jobQueue;
    private final DeadLetterQueue deadLetterQueue;
    private final long baseBackoffMillis;

    public RetryHandler(JobQueue jobQueue, DeadLetterQueue deadLetterQueue,
                       ScheduledExecutorService scheduler,
                       long baseBackoffMillis) {
        this.jobQueue = jobQueue;
        this.deadLetterQueue = deadLetterQueue;
        this.scheduler = scheduler;
        this.baseBackoffMillis = baseBackoffMillis;
    }

    /**
     * Computes the backoff delay for a given retry attempt (1-based).
     * Exponential backoff: base * 2^(attempt - 1).
     * Exposed so tests can verify the growth rate deterministically.
     */
    public long computeDelayMillis(int attemptNumber) {
        return baseBackoffMillis * (1L << (attemptNumber - 1));
    }

    public void schedule(Job job, int attemptNumber) {
        if (attemptNumber <= job.getMaxRetries()) {
            long delay = computeDelayMillis(attemptNumber); // exponential: base * 2^(n-1)
            System.out.println("[RetryHandler] job " + job.getId() +
                    " scheduled retry #" + attemptNumber + " after " + delay + " ms");
            try {
                scheduler.schedule(() -> jobQueue.submit(job), delay, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // Scheduler was shutdown; move to dead letter queue
                System.out.println("[RetryHandler] job " + job.getId() +
                        " failed; scheduler shutdown, moving to DLQ");
                deadLetterQueue.add(job);
            }
        } else {
            // Job exhausted all retries, move to dead letter queue
            System.out.println("[RetryHandler] job " + job.getId() +
                    " exhausted all retries, moving to DLQ");
            deadLetterQueue.add(job);
        }
    }
}