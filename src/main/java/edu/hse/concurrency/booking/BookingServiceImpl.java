package edu.hse.concurrency.booking;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class BookingServiceImpl implements BookingService {

    private final Hall hall;
    private final Duration holdDuration;
    private final SeatState[] seats;

    private final Object scheduler = new Object();
    private final TreeSet<Reservation> expirations = new TreeSet<>(
            Comparator.comparing((Reservation reservation) -> reservation.hold.expiresAt())
                    .thenComparingLong(reservation -> reservation.hold.id()));
    private final Set<Waiter> waiting = new HashSet<>();
    private long nextId;
    private volatile boolean stopped;
    private final Thread expirationThread;

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        this.hall = Objects.requireNonNull(hall, "hall");
        this.holdDuration = Objects.requireNonNull(holdDuration, "holdDuration");
        if (holdDuration.isZero() || holdDuration.isNegative()) {
            throw new IllegalArgumentException("holdDuration must be positive");
        }
        seats = new SeatState[hall.capacity()];
        for (int index = 0; index < seats.length; index++) {
            seats[index] = new SeatState();
        }
        expirationThread = new Thread(this::expireReservations, "booking-expiration");
        expirationThread.setDaemon(true);
        expirationThread.start();
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        Request request = validateRequest(clientId, seats);
        checkRunning();
        Attempt result = withSeats(request.indices, 0, () -> acquire(clientId, request, null));
        return Optional.ofNullable(result.hold);
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Request request = validateRequest(clientId, seats);
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        checkRunning();
        Waiter waiter = new Waiter(toNanosSaturated(timeout));
        try {
            for (;;) {
                Attempt result = withSeats(request.indices, 0, () -> {
                    waiter.reset();
                    return acquire(clientId, request, waiter);
                });
                if (result == Attempt.INTERRUPTED) {
                    Thread.interrupted();
                    throw new InterruptedException();
                }
                if (result != Attempt.WAIT) {
                    return Optional.ofNullable(result.hold);
                }
                waiter.awaitChange();
            }
        } finally {
            if (waiter.registered) {
                withSeats(request.indices, 0, () -> {
                    for (int index : request.indices) {
                        SeatState seat = this.seats[index];
                        seat.waiters.remove(waiter);
                        if (seat.waiters.isEmpty()) {
                            seat.waiters = null;
                        }
                    }
                    synchronized (scheduler) {
                        waiting.remove(waiter);
                    }
                    return null;
                });
            }
        }
    }

    @Override
    public boolean confirm(Hold hold) {
        return finish(hold, true);
    }

    @Override
    public boolean release(Hold hold) {
        return finish(hold, false);
    }

    @Override
    public SeatStatus status(Seat seat) {
        int index = validateSeat(seat);
        synchronized (seats[index]) {
            SeatState state = seats[index];
            if (state.sold) {
                return SeatStatus.SOLD;
            }
            return isActive(state.reservation, Instant.now()) ? SeatStatus.HELD : SeatStatus.FREE;
        }
    }

    @Override
    public void shutdown() {
        synchronized (scheduler) {
            if (!stopped) {
                stopped = true;
                expirations.clear();
                for (Waiter waiter : waiting) {
                    waiter.signal();
                }
                scheduler.notifyAll();
            }
        }
        boolean interrupted = false;
        for (;;) {
            try {
                expirationThread.join();
                break;
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private Attempt acquire(String clientId, Request request, Waiter waiter) {
        if (waiter != null && Thread.currentThread().isInterrupted()) {
            return Attempt.INTERRUPTED;
        }
        if (stopped) {
            return stoppedAttempt(waiter);
        }
        if (waiter != null && waiter.timeoutNanos != 0 && waiter.remainingNanos() <= 0) {
            return Attempt.EMPTY;
        }
        Instant now = Instant.now();
        boolean occupied = false;
        for (int index : request.indices) {
            SeatState seat = seats[index];
            if (seat.sold) {
                return Attempt.EMPTY;
            }
            occupied |= isActive(seat.reservation, now);
        }
        if (occupied) {
            if (waiter == null || waiter.timeoutNanos == 0) {
                return Attempt.EMPTY;
            }
            if (!waiter.registered) {
                waiter.registered = true;
                for (int index : request.indices) {
                    if (seats[index].waiters == null) {
                        seats[index].waiters = new HashSet<>();
                    }
                    seats[index].waiters.add(waiter);
                }
                synchronized (scheduler) {
                    if (stopped) {
                        return Attempt.EMPTY;
                    }
                    waiting.add(waiter);
                }
            }
            return Attempt.WAIT;
        }

        Reservation reservation;
        synchronized (scheduler) {
            if (waiter != null && Thread.currentThread().isInterrupted()) {
                return Attempt.INTERRUPTED;
            }
            if (stopped) {
                return stoppedAttempt(waiter);
            }
            if (waiter != null && waiter.timeoutNanos != 0 && waiter.remainingNanos() <= 0) {
                return Attempt.EMPTY;
            }
            Hold hold = new Hold(++nextId, clientId, request.seats, expiresAt(Instant.now()));
            reservation = new Reservation(hold, request.indices);
            expirations.add(reservation);
            if (expirations.first() == reservation) {
                scheduler.notifyAll();
            }
        }
        for (int index : request.indices) {
            seats[index].reservation = reservation;
        }
        return new Attempt(reservation.hold);
    }

    private boolean finish(Hold hold, boolean sell) {
        Objects.requireNonNull(hold, "hold");
        checkRunning();
        if (hold.seats().isEmpty()) {
            return false;
        }
        Seat first = hold.seats().iterator().next();
        if (!hall.contains(first)) {
            return false;
        }
        int firstIndex = validateSeat(first);
        Reservation reservation;
        synchronized (seats[firstIndex]) {
            reservation = seats[firstIndex].reservation;
            if (reservation == null || reservation.hold != hold) {
                return false;
            }
        }
        return withSeats(reservation.indices, 0, () -> {
            checkRunning();
            if (!isActive(reservation, Instant.now())) {
                return false;
            }
            for (int index : reservation.indices) {
                if (seats[index].reservation != reservation) {
                    return false;
                }
            }
            for (int index : reservation.indices) {
                seats[index].reservation = null;
                seats[index].sold = sell;
            }
            synchronized (scheduler) {
                if (expirations.remove(reservation)) {
                    scheduler.notifyAll();
                }
            }
            signalSeats(reservation.indices);
            return true;
        });
    }

    private void expireReservations() {
        for (;;) {
            Reservation reservation;
            synchronized (scheduler) {
                for (;;) {
                    if (stopped) {
                        return;
                    }
                    try {
                        if (expirations.isEmpty()) {
                            scheduler.wait();
                        } else {
                            Reservation first = expirations.first();
                            Duration delay = Duration.between(Instant.now(), first.hold.expiresAt());
                            if (delay.isNegative() || delay.isZero()) {
                                reservation = expirations.pollFirst();
                                break;
                            }
                            timedWait(scheduler, toNanosSaturated(delay));
                        }
                    } catch (InterruptedException ignored) {
                    }
                }
            }
            Reservation expired = reservation;
            withSeats(expired.indices, 0, () -> {
                for (int index : expired.indices) {
                    SeatState seat = seats[index];
                    if (seat.reservation == expired) {
                        seat.reservation = null;
                        signalSeat(seat);
                    }
                }
                return null;
            });
        }
    }

    private void signalSeats(int[] indices) {
        for (int index : indices) {
            signalSeat(seats[index]);
        }
    }

    private void signalSeat(SeatState seat) {
        if (seat.waiters != null) {
            for (Waiter waiter : seat.waiters) {
                waiter.signal();
            }
        }
    }

    private Request validateRequest(String clientId, Set<Seat> requestedSeats) {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(requestedSeats, "seats");
        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId must not be blank");
        }
        Set<Seat> snapshot = Set.copyOf(requestedSeats);
        if (snapshot.isEmpty() || snapshot.size() > MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException("A hold must contain 1 to " + MAX_SEATS_PER_HOLD + " seats");
        }
        int[] indices = new int[snapshot.size()];
        int position = 0;
        for (Seat seat : snapshot) {
            indices[position++] = validateSeat(seat);
        }
        Arrays.sort(indices);
        return new Request(snapshot, indices);
    }

    private int validateSeat(Seat seat) {
        Objects.requireNonNull(seat, "seat");
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Seat is outside the hall: " + seat);
        }
        return (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
    }

    private void checkRunning() {
        if (stopped) {
            throw new IllegalStateException("Booking service is shut down");
        }
    }

    private Attempt stoppedAttempt(Waiter waiter) {
        if (waiter == null) {
            throw new IllegalStateException("Booking service is shut down");
        }
        return Attempt.EMPTY;
    }

    private static boolean isActive(Reservation reservation, Instant now) {
        return reservation != null && now.isBefore(reservation.hold.expiresAt());
    }

    private Instant expiresAt(Instant issuedAt) {
        try {
            return issuedAt.plus(holdDuration);
        } catch (DateTimeException | ArithmeticException exception) {
            return Instant.MAX;
        }
    }

    private static long toNanosSaturated(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException exception) {
            return Long.MAX_VALUE;
        }
    }

    private static void timedWait(Object monitor, long nanos) throws InterruptedException {
        monitor.wait(nanos / 1_000_000, (int) (nanos % 1_000_000));
    }

    private <T> T withSeats(int[] indices, int position, LockedAction<T> action) {
        if (position == indices.length) {
            return action.run();
        }
        synchronized (seats[indices[position]]) {
            return withSeats(indices, position + 1, action);
        }
    }

    private interface LockedAction<T> {
        T run();
    }

    private static final class SeatState {
        private boolean sold;
        private Reservation reservation;
        private Set<Waiter> waiters;
    }

    private record Request(Set<Seat> seats, int[] indices) { }

    private record Reservation(Hold hold, int[] indices) { }

    private static final class Attempt {
        private static final Attempt EMPTY = new Attempt(null);
        private static final Attempt WAIT = new Attempt(null);
        private static final Attempt INTERRUPTED = new Attempt(null);
        private final Hold hold;

        private Attempt(Hold hold) {
            this.hold = hold;
        }
    }

    private final class Waiter {
        private final long timeoutNanos;
        private final long startedAt = System.nanoTime();
        private boolean registered;
        private boolean signalled;

        private Waiter(long timeoutNanos) {
            this.timeoutNanos = timeoutNanos;
        }

        private long remainingNanos() {
            return timeoutNanos - (System.nanoTime() - startedAt);
        }

        private synchronized void reset() {
            signalled = false;
        }

        private synchronized void signal() {
            signalled = true;
            notifyAll();
        }

        private synchronized void awaitChange() throws InterruptedException {
            while (!signalled && !stopped) {
                long remaining = remainingNanos();
                if (remaining <= 0) {
                    return;
                }
                timedWait(this, remaining);
            }
        }
    }
}
