package com.scheduler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstration entry point.
 *
 * Starts the scheduler, submits jobs with different priorities,
 * executes them via worker threads, shows status transitions,
 * simulates a failing job with retry and exponential backoff,
 * moves permanently failed jobs to the Dead Letter Queue,
 * and performs a graceful shutdown.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        JobScheduler scheduler = new JobScheduler(4, 3, 500L);

        System.out.println("=== Job Scheduler Demo ===\n");

        // 1. Submit jobs with different priorities
        String high = scheduler.submitJob("job-high", JobPriority.HIGH, () ->
                System.out.println("  [High] Job completed successfully."));
        String medium1 = scheduler.submitJob("job-medium-1", JobPriority.MEDIUM, () ->
                System.out.println("  [Medium] Job running."));
        String medium2 = scheduler.submitJob("job-medium-2", JobPriority.MEDIUM, () ->
                System.out.println("  [Medium] Job running."));
        String low = scheduler.submitJob("job-low", JobPriority.LOW, () ->
                System.out.println("  [Low] Job running."));

        // Let jobs run and complete
        Thread.sleep(1500);

        System.out.println("\n--- Job Statuses after first run ---");
        for (String id : new String[]{high, medium1, medium2, low}) {
            System.out.println(id + " -> " + scheduler.getJobStatus(id));
        }

        // 2. Simulate a failing job that exhausts all retries -> goes to DLQ
        String failing = scheduler.submitJob("job-fail", JobPriority.HIGH, () -> {
            throw new RuntimeException("Intentional failure for demo");
        }, 2); // maxRetries = 2

        System.out.println("\n--- Waiting for retry/backoff and DLQ move (~5s) ---");
        Thread.sleep(5000);

        Job failedJob = scheduler.getJob(failing);
        System.out.println("Job " + failing + " status: " + scheduler.getJobStatus(failing)
                + " | attempts: " + (failedJob != null ? failedJob.getAttempts() : "N/A"));
        System.out.println("Dead Letter Queue size: " + scheduler.getDeadLetterQueue().size());

        // 3. Show current DLQ contents
        System.out.println("\n--- Dead Letter Queue ---");
        System.out.println(scheduler.getDeadLetterQueue());

        // 4. Graceful shutdown
        System.out.println("\n--- Graceful Shutdown ---");
        scheduler.shutdown();

        // 5. Verify submissions are rejected after shutdown
        try {
            scheduler.submitJob("job-late", JobPriority.LOW, () -> {});
            System.out.println("ERROR: submission after shutdown should have thrown");
        } catch (IllegalStateException e) {
            System.out.println("Correctly rejected late submission: " + e.getMessage());
        }

        System.out.println("\n=== Demo Complete ===");
    }
}