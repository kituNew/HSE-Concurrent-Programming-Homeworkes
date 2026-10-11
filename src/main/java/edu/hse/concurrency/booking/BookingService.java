package edu.hse.concurrency.booking;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public interface BookingService {

    int MAX_SEATS_PER_HOLD = 10;

    Optional<Hold> hold(String clientId, Set<Seat> seats);

    Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException;

    boolean confirm(Hold hold);

    boolean release(Hold hold);

    SeatStatus status(Seat seat);

    void shutdown();
}
