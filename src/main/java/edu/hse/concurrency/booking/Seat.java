package edu.hse.concurrency.booking;

public record Seat(int row, int number) {

    public Seat {
        if (row < 1 || number < 1) {
            throw new IllegalArgumentException(
                    "Ряд и номер места должны быть положительными: " + row + "-" + number);
        }
    }

    @Override
    public String toString() {
        return row + "-" + number;
    }
}
