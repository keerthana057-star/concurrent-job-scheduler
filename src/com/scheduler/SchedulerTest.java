package com.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * SchedulerTest verifies the Concurrent Job Scheduler across 10 scenarios.
 * Self-contained: runs without JUnit. Reports PASS/FAIL per scenario.
 */
public class SchedulerTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=== SchedulerTest ===\n");
        testJobSubmission();
        testPriorityOrdering();
        testFifoEqualPriority();
        testConcurrentExecution();
        testJobStatusTransitions();
        testRetryAfterFailure();
        testExponentialBackoff();
        testDeadLetterQueue();
        testGracefulShutdown();
        testConcurrentSubmissions();
        summary();
        if (failed > 0) System.exit(1);
    }

    static void check(String name, boolean condition, String msg) {
        if (condition) {
            passed++;
            System.out.println("PASS: " + name);
        } else {
            failed++;
            System.out.println("FAIL: " + name + " -> " + msg);
        }
    }

    static JobStatus waitForStatus(JobScheduler scheduler, String jobId,
                                   JobStatus expected, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            last = scheduler.getJobStatus(jobId);
            if (last == expected) return last;
            Thread.sleep(50);
        }
        return last;
    }

    static void testJobSubmission() throws Exception {
        // Deterministic check of initial state (Job constructor sets PENDING)
        Job direct = new Job("d1", "direct", JobPriority.LOW, () -> {}, 0, 0);
        check("Job initial status PENDING",
                direct.getStatus() == JobStatus.PENDING,
                "expected PENDING, got " + direct.getStatus());

        JobScheduler scheduler = new JobScheduler(2, 3, 100L);
        String id = scheduler.submitJob("submit-test", JobPriority.MEDIUM, () -> {
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
        });
        check("Job submission",
                id != null && !id.isEmpty() && scheduler.getJob(id) != null, "job not registered");
        JobStatus finalStatus = waitForStatus(scheduler, id, JobStatus.COMPLETED, 3000);
        check("Submitted job reaches COMPLETED",
                finalStatus == JobStatus.COMPLETED, "expected COMPLETED, got " + finalStatus);
        scheduler.shutdown();
    }

    static void testPriorityOrdering() throws Exception {
        JobQueue queue = new JobQueue();
        Job jLow = new Job("a", "low", JobPriority.LOW, () -> {}, queue.nextSequence(), 0);
        Job jHigh = new Job("b", "high", JobPriority.HIGH, () -> {}, queue.nextSequence(), 0);
        Job jMed = new Job("c", "med", JobPriority.MEDIUM, () -> {}, queue.nextSequence(), 0);
        queue.submit(jLow);
        queue.submit(jHigh);
        queue.submit(jMed);
        List<String> order = new ArrayList<>();
        order.add(queue.take().getId());
        order.add(queue.take().getId());
        order.add(queue.take().getId());
        check("Priority ordering", order.equals(List.of("b", "c", "a")),
                "expected [high, med, low], got " + order);
    }

    static void testFifoEqualPriority() throws Exception {
        JobQueue queue = new JobQueue();
        Job first = new Job("f1", "first", JobPriority.MEDIUM, () -> {}, queue.nextSequence(), 0);
        Job second = new Job("f2", "second", JobPriority.MEDIUM, () -> {}, queue.nextSequence(), 0);
        queue.submit(first);
        queue.submit(second);
        List<String> order = new ArrayList<>();
        order.add(queue.take().getId());
        order.add(queue.take().getId());
        check("FIFO ordering for equal priority", order.equals(List.of("f1", "f2")),
                "expected [f1, f2], got " + order);
    }

    static void testConcurrentExecution() throws Exception {
        int numWorkers = 4;
        JobScheduler scheduler = new JobScheduler(numWorkers, 0, 100L);

        CountDownLatch arrived = new CountDownLatch(numWorkers);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        CountDownLatch finished = new CountDownLatch(numWorkers);

        for (int i = 0; i < numWorkers; i++) {
            scheduler.submitJob("conc-" + i, JobPriority.MEDIUM, () -> {
                try {
                    arrived.countDown();
                    gate.await();
                    int cur = active.incrementAndGet();
                    maxConcurrent.updateAndGet(m -> Math.max(m, cur));
                    Thread.sleep(80);
                    active.decrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    finished.countDown();
                }
            });
        }

        boolean allArrived = arrived.await(5, TimeUnit.SECONDS);
        gate.countDown();
        boolean allFinished = finished.await(5, TimeUnit.SECONDS);

        check("Concurrent execution (workers overlap)",
                allArrived && allFinished && maxConcurrent.get() >= 2,
                "maxConcurrent=" + maxConcurrent.get() + " (expected >= 2)");
        scheduler.shutdown();
    }

    static void testJobStatusTransitions() throws Exception {
        JobScheduler scheduler = new JobScheduler(2, 0, 100L);
        String id = scheduler.submitJob("status-test", JobPriority.LOW, () -> {
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
        });
        JobStatus finalStatus = waitForStatus(scheduler, id, JobStatus.COMPLETED, 3000);
        check("Job status transitions to COMPLETED",
                finalStatus == JobStatus.COMPLETED,
                "expected COMPLETED, got " + finalStatus);
        scheduler.shutdown();
    }

    static void testRetryAfterFailure() throws Exception {
        JobScheduler scheduler = new JobScheduler(2, 2, 150L);
        AtomicInteger failCounter = new AtomicInteger(0);
        String id = scheduler.submitJob("retry-test", JobPriority.HIGH, () -> {
            if (failCounter.getAndIncrement() == 0) throw new RuntimeException("fail once");
        }, 2);
        JobStatus finalStatus = waitForStatus(scheduler, id, JobStatus.COMPLETED, 4000);
        Job job = scheduler.getJob(id);
        check("Retry after failure ends COMPLETED",
                finalStatus == JobStatus.COMPLETED, "expected COMPLETED, got " + finalStatus);
        check("Job retried at least twice",
                job != null && job.getAttempts() >= 2,
                "attempts=" + (job != null ? job.getAttempts() : "null"));
        scheduler.shutdown();
    }

    static void testExponentialBackoff() {
        JobQueue queue = new JobQueue();
        DeadLetterQueue dlq = new DeadLetterQueue();
        ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();
        RetryHandler handler = new RetryHandler(queue, dlq, retryScheduler, 100L);
        long d1 = handler.computeDelayMillis(1);
        long d2 = handler.computeDelayMillis(2);
        long d3 = handler.computeDelayMillis(3);
        long d4 = handler.computeDelayMillis(4);
        retryScheduler.shutdownNow();
        check("Exponential backoff attempt 1 = base (100)", d1 == 100, "got " + d1);
        check("Exponential backoff attempt 2 = 2*base (200)", d2 == 200, "got " + d2);
        check("Exponential backoff attempt 3 = 4*base (400)", d3 == 400, "got " + d3);
        check("Exponential backoff attempt 4 = 8*base (800)", d4 == 800, "got " + d4);
    }

    static void testDeadLetterQueue() throws Exception {
        JobScheduler scheduler = new JobScheduler(2, 1, 100L);
        String id = scheduler.submitJob("dlq-test", JobPriority.HIGH, () -> {
            throw new RuntimeException("always fails");
        }, 1);
        long deadline = System.currentTimeMillis() + 4000;
        boolean inDlq = false;
        while (System.currentTimeMillis() < deadline) {
            if (scheduler.getDeadLetterQueue().size() >= 1) { inDlq = true; break; }
            Thread.sleep(50);
        }
        check("Job moves to Dead Letter Queue after retries exhausted", inDlq,
                "DLQ size=" + scheduler.getDeadLetterQueue().size());
        check("Job status is FAILED",
                scheduler.getJobStatus(id) == JobStatus.FAILED,
                "expected FAILED, got " + scheduler.getJobStatus(id));
        scheduler.shutdown();
    }

    static void testGracefulShutdown() throws Exception {
        JobScheduler scheduler = new JobScheduler(2, 0, 100L);
        scheduler.submitJob("shutdown-test", JobPriority.LOW, () -> {
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
        });
        Thread.sleep(300);
        scheduler.shutdown();
        check("Not accepting jobs after shutdown",
                !scheduler.isAcceptingJobs(), "still accepting jobs");
        boolean terminated = false;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (scheduler.isTerminated()) { terminated = true; break; }
            Thread.sleep(50);
        }
        check("Worker pool terminated after graceful shutdown", terminated, "pool not terminated");
        try {
            scheduler.submitJob("late", JobPriority.LOW, () -> {});
            check("Submission rejected after shutdown", false, "no exception thrown");
        } catch (IllegalStateException e) {
            check("Submission rejected after shutdown", true, "");
        }
    }

    static void testConcurrentSubmissions() throws Exception {
        JobScheduler scheduler = new JobScheduler(4, 0, 100L);
        int numJobs = 20;
        List<String> ids = new ArrayList<>(numJobs);
        CountDownLatch submitted = new CountDownLatch(numJobs);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numJobs; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                String id = scheduler.submitJob("concurrent-sub-" + idx, JobPriority.MEDIUM, () -> {
                    try { Thread.sleep(50); } catch (InterruptedException ignored) {}
                });
                synchronized (ids) { ids.add(id); }
                submitted.countDown();
            });
            threads.add(t);
            t.start();
        }

        boolean allSubmitted = submitted.await(10, TimeUnit.SECONDS);
        for (Thread t : threads) t.join(5000);

        int completed = 0;
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline) {
            completed = 0;
            for (String id : ids) {
                if (scheduler.getJobStatus(id) == JobStatus.COMPLETED) completed++;
            }
            if (completed == ids.size()) break;
            Thread.sleep(50);
        }

        check("Concurrent submissions all registered",
                allSubmitted && ids.size() == numJobs, "registered=" + ids.size());
        check("Concurrent submissions all COMPLETED",
                completed == numJobs, "completed=" + completed + "/" + ids.size());
        scheduler.shutdown();
    }

    static void summary() {
        System.out.println("\n=== Summary ===");
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);
        if (failed == 0) {
            System.out.println("All tests passed!");
        } else {
            System.out.println("Some tests FAILED.");
        }
    }
}