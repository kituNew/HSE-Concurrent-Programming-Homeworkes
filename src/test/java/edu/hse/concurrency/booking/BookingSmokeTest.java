package edu.hse.concurrency.booking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Проверяет только базовую, однопоточную работоспособность контракта {@link BookingService}.
 * Поведение под конкурентной нагрузкой (ожидание, таймауты, истечение брони, shutdown с
 * ожидающими потоками и т.д.) проверяется отдельными скрытыми тестами и здесь не затрагивается.
 */
class BookingSmokeTest {

    private BookingService service;

    @BeforeEach
    void setUp() {
        service = new BookingServiceImpl(new Hall(10, 10), Duration.ofMinutes(1));
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
    }

    @Test
    void freshSeatIsFree() {
        assertEquals(SeatStatus.FREE, service.status(new Seat(1, 1)));
    }

    @Test
    void holdMarksSeatsHeld() {
        Seat seat = new Seat(1, 1);

        Optional<Hold> hold = service.hold("alice", Set.of(seat));

        assertTrue(hold.isPresent());
        assertEquals(SeatStatus.HELD, service.status(seat));
        assertEquals("alice", hold.get().clientId());
        assertEquals(Set.of(seat), hold.get().seats());
    }

    @Test
    void holdFailsIfAnySeatAlreadyHeldAndChangesNothing() {
        Seat seat1 = new Seat(1, 1);
        Seat seat2 = new Seat(1, 2);
        service.hold("alice", Set.of(seat1));

        Optional<Hold> hold = service.hold("bob", Set.of(seat1, seat2));

        assertTrue(hold.isEmpty());
        assertEquals(SeatStatus.FREE, service.status(seat2));
    }

    @Test
    void confirmSellsSeatsAndCanBeCalledOnce() {
        Seat seat = new Seat(1, 1);
        Hold hold = service.hold("alice", Set.of(seat)).orElseThrow();

        assertTrue(service.confirm(hold));
        assertEquals(SeatStatus.SOLD, service.status(seat));
        assertFalse(service.confirm(hold));
    }

    @Test
    void releaseFreesSeatsAndCanBeCalledOnce() {
        Seat seat = new Seat(1, 1);
        Hold hold = service.hold("alice", Set.of(seat)).orElseThrow();

        assertTrue(service.release(hold));
        assertEquals(SeatStatus.FREE, service.status(seat));
        assertFalse(service.release(hold));
    }

    @Test
    void confirmFailsForAlreadyReleasedHold() {
        Seat seat = new Seat(1, 1);
        Hold hold = service.hold("alice", Set.of(seat)).orElseThrow();
        service.release(hold);

        assertFalse(service.confirm(hold));
    }

    @Test
    @Timeout(5)
    void holdOrWaitWithZeroTimeoutBehavesLikeHold() throws InterruptedException {
        Seat seat = new Seat(1, 1);
        service.hold("alice", Set.of(seat));

        assertTrue(service.holdOrWait("bob", Set.of(seat), Duration.ZERO).isEmpty());
    }

    @Test
    @Timeout(5)
    void shutdownRejectsMutationsButStatusKeepsWorking() {
        Seat seat = new Seat(1, 1);
        service.shutdown();

        assertThrows(IllegalStateException.class, () -> service.hold("alice", Set.of(seat)));
        assertThrows(IllegalStateException.class, () -> service.holdOrWait("alice", Set.of(seat), Duration.ZERO));
        assertDoesNotThrow(() -> service.status(seat));
        assertDoesNotThrow(service::shutdown);
    }

    @Test
    @Timeout(5)
    void invalidArgumentsAreRejectedWithoutChangingState() {
        Seat seat = new Seat(1, 1);

        assertThrows(IllegalArgumentException.class, () -> service.hold(" ", Set.of(seat)));
        assertThrows(IllegalArgumentException.class, () -> service.hold("alice", Set.of()));
        assertThrows(IllegalArgumentException.class, () -> service.hold("alice", Set.of(new Seat(100, 100))));
        assertThrows(IllegalArgumentException.class,
                () -> service.holdOrWait("alice", Set.of(seat), Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> service.hold(null, Set.of(seat)));
        assertThrows(NullPointerException.class, () -> service.hold("alice", null));

        assertEquals(SeatStatus.FREE, service.status(seat));
    }

    @Test
    void constructorValidatesHoldDuration() {
        Hall hall = new Hall(1, 1);

        assertThrows(IllegalArgumentException.class, () -> new BookingServiceImpl(hall, Duration.ZERO));
    }
}
