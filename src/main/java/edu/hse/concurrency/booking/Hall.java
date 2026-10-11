package edu.hse.concurrency.booking;


public record Hall(int rows, int seatsPerRow) {

    public static final int MAX_ROWS = 1000;
    public static final int MAX_SEATS_PER_ROW = 1000;

    public Hall {
        if (rows < 1 || rows > MAX_ROWS || seatsPerRow < 1 || seatsPerRow > MAX_SEATS_PER_ROW) {
            throw new IllegalArgumentException(
                    "Зал: от 1 до " + MAX_ROWS + " рядов и от 1 до " + MAX_SEATS_PER_ROW
                            + " мест в ряду, получено " + rows + "x" + seatsPerRow);
        }
    }

    public boolean contains(Seat seat) {
        return seat.row() <= rows && seat.number() <= seatsPerRow;
    }

    public int capacity() {
        return rows * seatsPerRow;
    }
}
