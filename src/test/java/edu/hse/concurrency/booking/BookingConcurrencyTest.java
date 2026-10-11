package edu.hse.concurrency.booking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(15)
class BookingConcurrencyTest {

    private static final Duration LONG_HOLD = Duration.ofMinutes(1);
    private static final Duration LONG_WAIT = Duration.ofSeconds(30);
    private final List<BookingService> services = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    @AfterEach
    void cleanUp() throws InterruptedException {
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (BookingService service : services) {
            service.shutdown();
        }
        for (Thread thread : threads) {
            thread.join(2_000);
            assertFalse(thread.isAlive(), "Test worker must terminate");
        }
    }

    @Test
    void releaseWakesWaiterAndAcquiresItsEntireSet() throws Exception {
        BookingService service = service(new Hall(2, 2), LONG_HOLD);
        Seat occupied = new Seat(1, 1);
        Seat other = new Seat(2, 2);
        Hold original = service.hold("first", Set.of(occupied)).orElseThrow();
        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "second", Set.of(new Seat(1, 1), other), LONG_WAIT));
        awaitWaiting(waiter);

        assertEquals(SeatStatus.FREE, service.status(other));
        assertTrue(service.release(original));

        Hold replacement = waiter.result().get(2, TimeUnit.SECONDS).orElseThrow();
        assertEquals(Set.of(occupied, other), replacement.seats());
        assertEquals(SeatStatus.HELD, service.status(occupied));
        assertEquals(SeatStatus.HELD, service.status(other));
        assertFalse(service.release(original));
        assertFalse(service.confirm(original));
    }

    @Test
    void releasingOnlyOneRequestedSeatDoesNotCreateAPartialHold() throws Exception {
        BookingService service = service(new Hall(1, 2), LONG_HOLD);
        Seat first = new Seat(1, 1);
        Seat second = new Seat(1, 2);
        Hold firstHold = service.hold("first", Set.of(first)).orElseThrow();
        Hold secondHold = service.hold("second", Set.of(second)).orElseThrow();
        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "both", Set.of(first, second), LONG_WAIT));
        awaitWaiting(waiter);

        assertTrue(service.release(firstHold));

        assertThrows(TimeoutException.class, () -> waiter.result().get(100, TimeUnit.MILLISECONDS));
        assertEquals(SeatStatus.FREE, service.status(first));
        assertEquals(SeatStatus.HELD, service.status(second));
        assertTrue(service.release(secondHold));
        assertEquals(Set.of(first, second), waiter.result().get(2, TimeUnit.SECONDS).orElseThrow().seats());
    }

    @Test
    void expiryWakesWaiterWithoutAnyOtherServiceCalls() throws Exception {
        BookingService service = service(new Hall(1, 1), Duration.ofMillis(750));
        Seat seat = new Seat(1, 1);
        Hold original = service.hold("first", Set.of(seat)).orElseThrow();
        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "second", Set.of(new Seat(1, 1)), LONG_WAIT));

        Hold replacement = waiter.result().get(4, TimeUnit.SECONDS).orElseThrow();

        assertTrue(replacement.expiresAt().isAfter(original.expiresAt()));
        assertFalse(service.confirm(original));
        assertFalse(service.release(original));
        assertEquals("second", replacement.clientId());
        assertFalse(service.status(seat) == SeatStatus.SOLD);
    }

    @Test
    void alreadySoldSeatRejectsLongWaitImmediatelyWithoutHoldingOtherSeats() throws Exception {
        BookingService service = service(new Hall(1, 2), LONG_HOLD);
        Seat sold = new Seat(1, 1);
        Seat free = new Seat(1, 2);
        assertTrue(service.confirm(service.hold("buyer", Set.of(sold)).orElseThrow()));

        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "late", Set.of(free, new Seat(1, 1)), LONG_WAIT));

        assertTrue(waiter.result().get(2, TimeUnit.SECONDS).isEmpty());
        assertEquals(SeatStatus.SOLD, service.status(sold));
        assertEquals(SeatStatus.FREE, service.status(free));
    }

    @Test
    void confirmingBlockingHoldWakesWaiterWithEmptyResult() throws Exception {
        BookingService service = service(new Hall(1, 2), LONG_HOLD);
        Seat occupied = new Seat(1, 1);
        Seat free = new Seat(1, 2);
        Hold original = service.hold("buyer", Set.of(occupied)).orElseThrow();
        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "late", Set.of(occupied, free), LONG_WAIT));
        awaitWaiting(waiter);

        assertTrue(service.confirm(original));

        assertTrue(waiter.result().get(2, TimeUnit.SECONDS).isEmpty());
        assertEquals(SeatStatus.SOLD, service.status(occupied));
        assertEquals(SeatStatus.FREE, service.status(free));
    }

    @Test
    void timeoutDoesNotAcquireAnySeatsOrReturnImmediately() throws Exception {
        BookingService service = service(new Hall(1, 2), LONG_HOLD);
        Seat occupied = new Seat(1, 1);
        Seat free = new Seat(1, 2);
        service.hold("first", Set.of(occupied)).orElseThrow();
        long started = System.nanoTime();

        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "second", Set.of(occupied, free), Duration.ofMillis(250)));

        assertTrue(waiter.result().get(3, TimeUnit.SECONDS).isEmpty());
        assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(150));
        assertEquals(SeatStatus.HELD, service.status(occupied));
        assertEquals(SeatStatus.FREE, service.status(free));
    }

    @Test
    void veryLargeTimeoutDoesNotOverflowAndCanBeWokenByRelease() throws Exception {
        BookingService service = service(new Hall(1, 1), LONG_HOLD);
        Seat seat = new Seat(1, 1);
        Hold original = service.hold("first", Set.of(seat)).orElseThrow();
        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "second", Set.of(seat), Duration.ofSeconds(Long.MAX_VALUE)));
        awaitWaiting(waiter);

        assertTrue(service.release(original));

        assertTrue(waiter.result().get(2, TimeUnit.SECONDS).isPresent());
    }

    @Test
    void interruptionThrowsAndDoesNotLeaveAPartialBooking() throws Exception {
        BookingService service = service(new Hall(1, 2), LONG_HOLD);
        Seat occupied = new Seat(1, 1);
        Seat free = new Seat(1, 2);
        Hold original = service.hold("first", Set.of(occupied)).orElseThrow();
        Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(
                "interrupted", Set.of(occupied, free), LONG_WAIT));
        awaitWaiting(waiter);

        waiter.thread().interrupt();

        ExecutionException error = assertThrows(ExecutionException.class,
                () -> waiter.result().get(2, TimeUnit.SECONDS));
        assertInstanceOf(InterruptedException.class, error.getCause());
        assertEquals(SeatStatus.FREE, service.status(free));
        assertTrue(service.release(original));
        assertTrue(service.hold("next", Set.of(occupied, free)).isPresent());
    }

    @Test
    void shutdownWakesAllRegisteredWaitersAndRejectsFurtherMutations() throws Exception {
        BookingService service = service(new Hall(1, 1), LONG_HOLD);
        Seat seat = new Seat(1, 1);
        Hold original = service.hold("first", Set.of(seat)).orElseThrow();
        List<Task<Optional<Hold>>> waiters = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            String client = "waiter-" + i;
            Task<Optional<Hold>> waiter = task(() -> service.holdOrWait(client, Set.of(seat), LONG_WAIT));
            awaitWaiting(waiter);
            waiters.add(waiter);
        }

        service.shutdown();

        for (Task<Optional<Hold>> waiter : waiters) {
            assertTrue(waiter.result().get(2, TimeUnit.SECONDS).isEmpty());
        }
        assertThrows(IllegalStateException.class, () -> service.hold("new", Set.of(seat)));
        assertThrows(IllegalStateException.class,
                () -> service.holdOrWait("new", Set.of(seat), Duration.ZERO));
        assertThrows(IllegalStateException.class, () -> service.confirm(original));
        assertThrows(IllegalStateException.class, () -> service.release(original));
        assertDoesNotThrow(() -> service.status(seat));
        assertDoesNotThrow(service::shutdown);
    }

    @Test
    void concurrentShutdownCallsAllCompleteAndAreIdempotent() throws Exception {
        BookingService service = service(new Hall(1, 1), LONG_HOLD);
        int count = 12;
        CyclicBarrier start = new CyclicBarrier(count);
        List<Task<Void>> shutdowns = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            shutdowns.add(task(() -> {
                start.await(5, TimeUnit.SECONDS);
                service.shutdown();
                return null;
            }));
        }

        for (Task<Void> shutdown : shutdowns) {
            shutdown.result().get(5, TimeUnit.SECONDS);
        }
        assertDoesNotThrow(service::shutdown);
        assertEquals(SeatStatus.FREE, service.status(new Seat(1, 1)));
    }

    @Test
    void sameSeatHasExactlyOneHoldAndOneSuccessfulConfirmationUnderContention() throws Exception {
        BookingService service = service(new Hall(1, 1), LONG_HOLD);
        int count = 32;
        CyclicBarrier holdStart = new CyclicBarrier(count);
        List<Task<Optional<Hold>>> attempts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String client = "client-" + i;
            attempts.add(task(() -> {
                holdStart.await(5, TimeUnit.SECONDS);
                return service.hold(client, Set.of(new Seat(1, 1)));
            }));
        }
        List<Hold> winners = new ArrayList<>();
        for (Task<Optional<Hold>> attempt : attempts) {
            attempt.result().get(5, TimeUnit.SECONDS).ifPresent(winners::add);
        }
        assertEquals(1, winners.size());

        Hold winner = winners.get(0);
        CyclicBarrier confirmStart = new CyclicBarrier(count);
        List<Task<Boolean>> confirmations = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            confirmations.add(task(() -> {
                confirmStart.await(5, TimeUnit.SECONDS);
                return service.confirm(winner);
            }));
        }
        int successes = 0;
        for (Task<Boolean> confirmation : confirmations) {
            if (confirmation.result().get(5, TimeUnit.SECONDS)) {
                successes++;
            }
        }
        assertEquals(1, successes);
        assertEquals(SeatStatus.SOLD, service.status(new Seat(1, 1)));
    }

    @Test
    void overlappingRequestsWithDifferentIterationOrdersHaveNoPartialEffects() throws Exception {
        int count = 24;
        BookingService service = service(new Hall(1, count + 1), LONG_HOLD);
        Seat shared = new Seat(1, 1);
        CyclicBarrier start = new CyclicBarrier(count);
        List<Set<Seat>> requests = new ArrayList<>();
        List<Task<Optional<Hold>>> attempts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Seat own = new Seat(1, i + 2);
            Set<Seat> requested = new LinkedHashSet<>();
            requested.add(i % 2 == 0 ? shared : own);
            requested.add(i % 2 == 0 ? own : shared);
            requests.add(requested);
            String client = "client-" + i;
            attempts.add(task(() -> {
                start.await(5, TimeUnit.SECONDS);
                return service.hold(client, requested);
            }));
        }
        List<Hold> winners = new ArrayList<>();
        for (Task<Optional<Hold>> attempt : attempts) {
            attempt.result().get(5, TimeUnit.SECONDS).ifPresent(winners::add);
        }

        assertEquals(1, winners.size());
        Set<Seat> winningSeats = winners.get(0).seats();
        for (Set<Seat> requested : requests) {
            for (Seat seat : requested) {
                assertEquals(winningSeats.contains(seat) ? SeatStatus.HELD : SeatStatus.FREE,
                        service.status(new Seat(seat.row(), seat.number())));
            }
        }
    }

    @Test
    void foreignAndForgedHoldsCannotConfirmOrReleaseAnAuthenticHold() {
        BookingService first = service(new Hall(1, 2), LONG_HOLD);
        BookingService second = service(new Hall(1, 2), LONG_HOLD);
        Seat seat = new Seat(1, 1);
        Hold authentic = first.hold("owner", Set.of(seat)).orElseThrow();
        Hold foreign = second.hold("owner", Set.of(seat)).orElseThrow();
        Hold copied = new Hold(authentic.id(), authentic.clientId(), authentic.seats(), authentic.expiresAt());
        Hold changedSeats = new Hold(authentic.id(), authentic.clientId(),
                Set.of(new Seat(1, 2)), authentic.expiresAt());
        Hold extended = new Hold(authentic.id(), authentic.clientId(), authentic.seats(), Instant.MAX);
        Hold blankClient = new Hold(authentic.id(), " ", authentic.seats(), Instant.MAX);
        Hold emptySeats = new Hold(authentic.id(), authentic.clientId(), Set.of(), Instant.MAX);
        Hold outsideHall = new Hold(authentic.id(), authentic.clientId(),
                Set.of(new Seat(2, 1)), Instant.MAX);

        for (Hold invalid : List.of(foreign, copied, changedSeats, extended,
                blankClient, emptySeats, outsideHall)) {
            assertFalse(first.confirm(invalid));
            assertFalse(first.release(invalid));
        }
        assertEquals(SeatStatus.HELD, first.status(seat));
        assertEquals(SeatStatus.FREE, first.status(new Seat(1, 2)));
        assertEquals(SeatStatus.HELD, second.status(seat));
        assertTrue(first.confirm(authentic));
        assertTrue(second.release(foreign));
    }

    @Test
    void validatesAllArgumentsBeforeCheckingWhetherServiceStopped() {
        BookingService service = service(new Hall(2, 10), LONG_HOLD);
        Seat seat = new Seat(1, 1);
        Set<Seat> withNull = new HashSet<>();
        withNull.add(seat);
        withNull.add(null);
        Set<Seat> tooMany = new HashSet<>();
        for (int number = 1; number <= 10; number++) {
            tooMany.add(new Seat(1, number));
        }
        tooMany.add(new Seat(2, 1));
        List<Hold> malformed = List.of(
                new Hold(1, " ", Set.of(seat), Instant.MAX),
                new Hold(1, "client", Set.of(), Instant.MAX),
                new Hold(1, "client", tooMany, Instant.MAX),
                new Hold(1, "client", Set.of(new Seat(3, 1)), Instant.MAX));
        service.shutdown();

        assertThrows(NullPointerException.class, () -> service.hold(null, Set.of(seat)));
        assertThrows(NullPointerException.class, () -> service.hold("client", null));
        assertThrows(NullPointerException.class, () -> service.hold("client", withNull));
        assertThrows(IllegalArgumentException.class, () -> service.hold(" \t\n", Set.of(seat)));
        assertThrows(IllegalArgumentException.class, () -> service.hold("client", Set.of()));
        assertThrows(IllegalArgumentException.class, () -> service.hold("client", tooMany));
        assertThrows(IllegalArgumentException.class,
                () -> service.hold("client", Set.of(new Seat(3, 1))));
        assertThrows(NullPointerException.class,
                () -> service.holdOrWait("client", Set.of(seat), null));
        assertThrows(NullPointerException.class,
                () -> service.holdOrWait(null, Set.of(seat), Duration.ZERO));
        assertThrows(NullPointerException.class,
                () -> service.holdOrWait("client", null, Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> service.holdOrWait("client", Set.of(seat), Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class,
                () -> service.holdOrWait("client", Set.of(), Duration.ZERO));
        assertThrows(NullPointerException.class, () -> service.confirm(null));
        assertThrows(NullPointerException.class, () -> service.release(null));
        for (Hold invalid : malformed) {
            assertThrows(IllegalStateException.class, () -> service.confirm(invalid));
            assertThrows(IllegalStateException.class, () -> service.release(invalid));
        }
        assertThrows(NullPointerException.class, () -> service.status(null));
        assertThrows(IllegalArgumentException.class, () -> service.status(new Seat(1, 11)));
        assertEquals(SeatStatus.FREE, service.status(seat));
    }

    @Test
    void invalidMultiSeatRequestDoesNotChangeAValidSeat() {
        BookingService service = service(new Hall(1, 2), LONG_HOLD);
        Seat valid = new Seat(1, 1);
        Set<Seat> requested = new LinkedHashSet<>();
        requested.add(valid);
        requested.add(new Seat(2, 1));

        assertThrows(IllegalArgumentException.class, () -> service.hold("client", requested));
        assertThrows(IllegalArgumentException.class,
                () -> service.holdOrWait("client", requested, Duration.ZERO));
        assertEquals(SeatStatus.FREE, service.status(valid));
    }

    @Test
    void constructorRejectsNullAndNegativeDuration() {
        assertThrows(NullPointerException.class, () -> new BookingServiceImpl(null, LONG_HOLD));
        assertThrows(NullPointerException.class, () -> new BookingServiceImpl(new Hall(1, 1), null));
        assertThrows(IllegalArgumentException.class,
                () -> new BookingServiceImpl(new Hall(1, 1), Duration.ofNanos(-1)));
    }

    private BookingService service(Hall hall, Duration holdDuration) {
        BookingService service = new BookingServiceImpl(hall, holdDuration);
        services.add(service);
        return service;
    }

    private <T> Task<T> task(Callable<T> body) throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        FutureTask<T> future = new FutureTask<>(() -> {
            entered.countDown();
            return body.call();
        });
        Thread thread = new Thread(future, "booking-contract-test-" + threads.size());
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS), "Worker did not start");
        return new Task<>(thread, future);
    }

    private static void awaitWaiting(Task<?> task) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (task.result().isDone()) {
                task.result().get();
                fail("holdOrWait returned before the held seat was freed");
            }
            Thread.State state = task.thread().getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(1);
        }
        fail("holdOrWait did not enter a waiting state");
    }

    private record Task<T>(Thread thread, FutureTask<T> result) {
    }
}
