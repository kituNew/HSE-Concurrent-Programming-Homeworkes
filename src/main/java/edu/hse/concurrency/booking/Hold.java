package edu.hse.concurrency.booking;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public record Hold(long id, String clientId, Set<Seat> seats, Instant expiresAt) {

    public Hold {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(expiresAt, "expiresAt");
        seats = Set.copyOf(seats);
    }
}
