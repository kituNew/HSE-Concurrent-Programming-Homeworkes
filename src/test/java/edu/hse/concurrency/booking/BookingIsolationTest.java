package edu.hse.concurrency.booking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(15)
class BookingIsolationTest {

    private final List<BookingServiceImpl> services = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    @AfterEach
    void cleanUp() throws InterruptedException {
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (BookingServiceImpl service : services) {
            service.shutdown();
        }
        for (Thread thread : threads) {
            thread.join(2_000);
            assertFalse(thread.isAlive(), "Test worker must terminate");
        }
    }

    @Test
    void unrelatedSeatChangesDoNotRepeatedlyWakeTheWaitingCustomer() throws Exception {
        BookingServiceImpl service = service(Duration.ofMinutes(1));
        Seat requested = new Seat(1, 1);
        Seat unrelated = new Seat(1, 2);
        Hold blocker = service.hold("blocker", Set.of(requested)).orElseThrow();
        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "waiting", Set.of(requested), Duration.ofSeconds(30)));
        awaitState(waiter.thread(), Thread.State.WAITING, Thread.State.TIMED_WAITING);
        ThreadMXBean threadsBean = ManagementFactory.getThreadMXBean();
        long before = waitedCount(threadsBean, waiter.thread());

        Task<Void> unrelatedChanges = task(() -> {
            for (int i = 0; i < 200; i++) {
                Hold other = service.hold("unrelated", Set.of(unrelated)).orElseThrow();
                assertTrue(service.release(other));
                new CountDownLatch(1).await(1, TimeUnit.MILLISECONDS);
            }
            return null;
        });
        unrelatedChanges.result().get(5, TimeUnit.SECONDS);
        assertFalse(waiter.result().isDone());
        long extraWaits = waitedCount(threadsBean, waiter.thread()) - before;

        assertTrue(extraWaits <= 2,
                "Unrelated seats repeatedly woke this customer: " + extraWaits + " new waits");
        assertTrue(service.release(blocker));
        assertTrue(waiter.result().get(2, TimeUnit.SECONDS).isPresent());
    }

    @Test
    void blockedSeatOperationDoesNotPreventOperationsOnAnIndependentSeat() throws Exception {
        BookingServiceImpl service = service(Duration.ofMinutes(1));
        Seat delayedSeat = new Seat(1, 1);
        Seat independentSeat = new Seat(1, 2);
        Object delayedMonitor = seatMonitor(service, 0);
        CountDownLatch monitorHeld = new CountDownLatch(1);
        CountDownLatch releaseMonitor = new CountDownLatch(1);
        Task<Void> monitorHolder = task(() -> {
            synchronized (delayedMonitor) {
                monitorHeld.countDown();
                assertTrue(releaseMonitor.await(5, TimeUnit.SECONDS));
            }
            return null;
        });
        assertTrue(monitorHeld.await(5, TimeUnit.SECONDS));
        Task<Optional<Hold>> delayed = task(() -> service.hold("delayed", Set.of(delayedSeat)));

        try {
            awaitState(delayed.thread(), Thread.State.BLOCKED);
            Task<Void> independent = task(() -> {
                Hold released = service.hold("independent", Set.of(independentSeat)).orElseThrow();
                assertEquals(SeatStatus.HELD, service.status(independentSeat));
                assertTrue(service.release(released));
                Hold sold = service.hold("independent", Set.of(independentSeat)).orElseThrow();
                assertTrue(service.confirm(sold));
                assertEquals(SeatStatus.SOLD, service.status(independentSeat));
                return null;
            });
            independent.result().get(2, TimeUnit.SECONDS);
            assertFalse(delayed.result().isDone(), "The deliberately blocked operation must still be blocked");
        } finally {
            releaseMonitor.countDown();
        }

        monitorHolder.result().get(2, TimeUnit.SECONDS);
        assertTrue(delayed.result().get(2, TimeUnit.SECONDS).isPresent());
    }

    @Test
    void shutdownWaitsUntilAnAlreadyRunningExpirationFinishes() throws Exception {
        checkShutdownJoinsWorker(false);
    }

    @Test
    void interruptedShutdownStillJoinsWorkerAndRestoresInterruptStatus() throws Exception {
        checkShutdownJoinsWorker(true);
    }

    @Test
    void thousandsOfOverlappingRequestsNeverHaveTwoLiveOwnersOfASeat() throws Exception {
        int capacity = 24;
        int customerCount = 24;
        BookingServiceImpl service = new BookingServiceImpl(new Hall(1, capacity), Duration.ofMinutes(1));
        services.add(service);
        AtomicIntegerArray owners = new AtomicIntegerArray(capacity);
        AtomicInteger successfulHolds = new AtomicInteger();
        CyclicBarrier start = new CyclicBarrier(customerCount);
        List<Task<Void>> customers = new ArrayList<>();
        for (int customer = 0; customer < customerCount; customer++) {
            int ownerId = customer + 1;
            customers.add(task(() -> {
                start.await(5, TimeUnit.SECONDS);
                for (int iteration = 0; iteration < 150; iteration++) {
                    int first = (ownerId * 7 + iteration * 5) % capacity;
                    int second = (first + 1 + iteration % 5) % capacity;
                    Set<Seat> requested = Set.of(new Seat(1, first + 1), new Seat(1, second + 1));
                    Optional<Hold> attempt = iteration % 2 == 0
                            ? service.hold("customer-" + ownerId, requested)
                            : service.holdOrWait("customer-" + ownerId, requested, Duration.ofMillis(25));
                    if (attempt.isEmpty()) {
                        continue;
                    }
                    Hold hold = attempt.orElseThrow();
                    List<Integer> claimed = new ArrayList<>(2);
                    try {
                        for (Seat seat : hold.seats()) {
                            int index = seat.number() - 1;
                            assertTrue(owners.compareAndSet(index, 0, ownerId),
                                    "Seat " + seat + " is held by multiple customers");
                            claimed.add(index);
                            assertEquals(SeatStatus.HELD, service.status(seat));
                        }
                        successfulHolds.incrementAndGet();
                        new CountDownLatch(1).await(1, TimeUnit.MILLISECONDS);
                    } finally {
                        for (int index : claimed) {
                            assertTrue(owners.compareAndSet(index, ownerId, 0));
                        }
                        assertTrue(service.release(hold));
                    }
                }
                return null;
            }));
        }

        for (Task<Void> customer : customers) {
            customer.result().get(10, TimeUnit.SECONDS);
        }
        assertTrue(successfulHolds.get() > 0);
        for (int index = 0; index < capacity; index++) {
            assertEquals(0, owners.get(index));
            assertEquals(SeatStatus.FREE, service.status(new Seat(1, index + 1)));
        }
    }

    private void checkShutdownJoinsWorker(boolean interruptCaller) throws Exception {
        BookingServiceImpl service = service(Duration.ofMillis(250));
        Thread worker = expirationThread(service);
        Task<Boolean> shutdown;
        synchronized (seatMonitor(service, 0)) {
            service.hold("expiring", Set.of(new Seat(1, 1))).orElseThrow();
            awaitState(worker, Thread.State.BLOCKED);
            shutdown = task(() -> {
                if (interruptCaller) {
                    Thread.currentThread().interrupt();
                }
                service.shutdown();
                assertFalse(worker.isAlive(), "shutdown returned while its worker was still alive");
                return Thread.currentThread().isInterrupted();
            });
            awaitState(shutdown.thread(), Thread.State.WAITING, Thread.State.TIMED_WAITING);
            assertFalse(shutdown.result().isDone());
            assertTrue(worker.isAlive());
        }

        assertEquals(interruptCaller, shutdown.result().get(3, TimeUnit.SECONDS));
        assertFalse(worker.isAlive());
    }

    private BookingServiceImpl service(Duration holdDuration) {
        BookingServiceImpl service = new BookingServiceImpl(new Hall(1, 2), holdDuration);
        services.add(service);
        return service;
    }

    private <T> Task<T> task(Callable<T> body) throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        FutureTask<T> future = new FutureTask<>(() -> {
            entered.countDown();
            return body.call();
        });
        Thread thread = new Thread(future, "booking-isolation-test-" + threads.size());
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        return new Task<>(thread, future);
    }

    private static Object seatMonitor(BookingServiceImpl service, int index) throws Exception {
        Field field = BookingServiceImpl.class.getDeclaredField("seats");
        field.setAccessible(true);
        return ((Object[]) field.get(service))[index];
    }

    private static Thread expirationThread(BookingServiceImpl service) throws Exception {
        Field field = BookingServiceImpl.class.getDeclaredField("expirationThread");
        field.setAccessible(true);
        return (Thread) field.get(service);
    }

    private static long waitedCount(ThreadMXBean threadsBean, Thread thread) {
        ThreadInfo info = threadsBean.getThreadInfo(thread.getId());
        assertNotNull(info, "The waiting customer unexpectedly terminated");
        return info.getWaitedCount();
    }

    private static void awaitState(Thread thread, Thread.State... expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread.State actual = thread.getState();
            for (Thread.State state : expected) {
                if (actual == state) {
                    return;
                }
            }
            assertTrue(thread.isAlive(), "Worker terminated before reaching the expected state");
            Thread.sleep(1);
        }
        fail("Worker did not reach the expected state; current state: " + thread.getState());
    }

    private record Task<T>(Thread thread, FutureTask<T> result) {
    }
}
