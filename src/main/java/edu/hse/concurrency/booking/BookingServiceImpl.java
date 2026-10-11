package edu.hse.concurrency.booking;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Supplier;

public class BookingServiceImpl implements BookingService {

    private static final Duration MAX_NANOS = Duration.ofNanos(Long.MAX_VALUE);

    private final Hall hall;
    private final Duration holdDuration;
    private final Slot[] slots;
    private final Expirations expirations = new Expirations();
    private final Thread expirer;
    private volatile boolean stopped;

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        Objects.requireNonNull(hall, "hall");
        Objects.requireNonNull(holdDuration, "holdDuration");

        if (holdDuration.isNegative() || holdDuration.isZero()) {
            throw new IllegalArgumentException("Время брони должно быть положительным: " + holdDuration);
        }

        this.hall = hall;
        this.holdDuration = holdDuration;
        this.slots = new Slot[hall.capacity()];

        for (int i = 0; i < slots.length; i++) {
            slots[i] = new Slot();
        }

        this.expirer = new Thread(this::expireHolds, "booking-expirer");
        expirer.setDaemon(true);
        expirer.start();
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        Set<Seat> request = checkRequest(clientId, seats);
        int[] indices = indicesOf(request);
        ensureRunning();
        return locked(indices, () -> tryHold(clientId, request, indices, Instant.now()));
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        Set<Seat> request = checkRequest(clientId, seats);
        int[] indices = indicesOf(request);

        if (timeout.isNegative()) {
            throw new IllegalArgumentException("Таймаут не может быть отрицательным: " + timeout);
        }

        ensureRunning();
        long deadline = System.nanoTime() + (timeout.compareTo(MAX_NANOS) < 0 ? timeout.toNanos() : Long.MAX_VALUE);
        Waiter waiter = new Waiter();

        while (true) {
            Optional<Hold> result = locked(indices, () -> attempt(clientId, request, indices, waiter, deadline));

            if (result != null) {
                return result;
            }

            try {
                waiter.await(deadline);
            } catch (InterruptedException e) {
                locked(indices, () -> {
                    forget(indices, waiter);
                    return null;
                });

                throw e;
            }
        }
    }

    @Override
    public boolean confirm(Hold hold) {
        return settle(hold, true);
    }

    @Override
    public boolean release(Hold hold) {
        return settle(hold, false);
    }

    @Override
    public SeatStatus status(Seat seat) {
        Objects.requireNonNull(seat, "seat");
        checkInHall(seat);
        Slot slot = slots[indexOf(seat)];

        synchronized (slot) {
            if (slot.sold) {
                return SeatStatus.SOLD;
            }

            return slot.isFree(Instant.now()) ? SeatStatus.FREE : SeatStatus.HELD;
        }
    }

    @Override
    public void shutdown() {
        if (!expirations.stop()) {
            return;
        }

        for (Slot slot : slots) {
            synchronized (slot) {
                slot.wakeWaiters();
            }
        }

        boolean interrupted = false;

        while (expirer.isAlive()) {
            try {
                expirer.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private Optional<Hold> attempt(String clientId, Set<Seat> seats, int[] indices, Waiter waiter, long deadline) {
        forget(indices, waiter);

        if (stopped) {
            return Optional.empty();
        }

        for (int i : indices) {
            if (slots[i].sold) {
                return Optional.empty();
            }
        }

        Instant now = Instant.now();
        Optional<Hold> hold = tryHold(clientId, seats, indices, now);

        if (hold.isPresent() || deadline - System.nanoTime() <= 0) {
            return hold;
        }

        for (int i : indices) {
            if (!slots[i].isFree(now)) {
                slots[i].addWaiter(waiter);
            }
        }

        return null;
    }

    private Optional<Hold> tryHold(String clientId, Set<Seat> seats, int[] indices, Instant now) {
        for (int i : indices) {
            if (!slots[i].isFree(now)) {
                return Optional.empty();
            }
        }

        Instant expiresAt = holdDuration.compareTo(Duration.between(now, Instant.MAX)) < 0
                ? now.plus(holdDuration)
                : Instant.MAX;
        Hold hold = expirations.issue(clientId, seats, expiresAt);

        for (int i : indices) {
            slots[i].holder = hold;
        }

        return Optional.of(hold);
    }

    private boolean settle(Hold hold, boolean sell) {
        Objects.requireNonNull(hold, "hold");
        ensureRunning();
        Set<Seat> seats = hold.seats();

        if (seats.isEmpty() || seats.size() > MAX_SEATS_PER_HOLD || !seats.stream().allMatch(hall::contains)) {
            return false;
        }

        int[] indices = indicesOf(seats);
        return locked(indices, () -> Instant.now().isBefore(hold.expiresAt()) && free(hold, indices, sell));
    }

    private boolean free(Hold hold, int[] indices, boolean sell) {
        for (int i : indices) {
            if (slots[i].holder != hold) {
                return false;
            }
        }

        for (int i : indices) {
            slots[i].free(sell);
        }

        return true;
    }

    private void forget(int[] indices, Waiter waiter) {
        for (int i : indices) {
            slots[i].removeWaiter(waiter);
        }
    }

    private void expireHolds() {
        try {
            Hold hold;

            while ((hold = expirations.next()) != null) {
                expire(hold);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void expire(Hold hold) {
        int[] indices = indicesOf(hold.seats());
        locked(indices, () -> free(hold, indices, false));
    }

    private <T> T locked(int[] indices, Supplier<T> action) {
        return locked(indices, 0, action);
    }

    private <T> T locked(int[] indices, int from, Supplier<T> action) {
        if (from == indices.length) {
            return action.get();
        }

        synchronized (slots[indices[from]]) {
            return locked(indices, from + 1, action);
        }
    }

    private Set<Seat> checkRequest(String clientId, Set<Seat> seats) {
        Objects.requireNonNull(clientId, "clientId");
        Set<Seat> request = Set.copyOf(Objects.requireNonNull(seats, "seats"));

        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId не может быть пустым");
        }

        if (request.isEmpty() || request.size() > MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException(
                    "В брони должно быть от 1 до " + MAX_SEATS_PER_HOLD + " мест, получено " + request.size());
        }

        request.forEach(this::checkInHall);
        return request;
    }

    private void checkInHall(Seat seat) {
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Места " + seat + " нет в зале " + hall.rows() + "x" + hall.seatsPerRow());
        }
    }

    private int[] indicesOf(Set<Seat> seats) {
        return seats.stream().mapToInt(this::indexOf).sorted().toArray();
    }

    private int indexOf(Seat seat) {
        return (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException("Сервис остановлен");
        }
    }

    private static long waitMillis(Duration left) {
        return left.getSeconds() < Long.MAX_VALUE / 1000 - 1 ? left.toMillis() + 1 : Long.MAX_VALUE;
    }

    private static final class Slot {
        private Hold holder;
        private boolean sold;
        private List<Waiter> waiters;

        boolean isFree(Instant now) {
            return !sold && (holder == null || !now.isBefore(holder.expiresAt()));
        }

        void free(boolean sell) {
            holder = null;
            sold = sell;
            wakeWaiters();
        }

        void addWaiter(Waiter waiter) {
            if (waiters == null) {
                waiters = new ArrayList<>();
            }

            waiters.add(waiter);
        }

        void removeWaiter(Waiter waiter) {
            if (waiters != null) {
                waiters.remove(waiter);
            }
        }

        void wakeWaiters() {
            if (waiters == null) {
                return;
            }

            waiters.forEach(Waiter::signal);
            waiters.clear();
        }
    }

    private final class Waiter {
        private boolean signalled;

        synchronized void signal() {
            signalled = true;
            notify();
        }

        synchronized void await(long deadline) throws InterruptedException {
            long left;

            while (!signalled && !stopped && (left = deadline - System.nanoTime()) > 0) {
                wait(waitMillis(Duration.ofNanos(left)));
            }

            signalled = false;
        }
    }

    private final class Expirations {
        private final PriorityQueue<Hold> queue = new PriorityQueue<>(Comparator.comparing(Hold::expiresAt));
        private long nextId;

        synchronized Hold issue(String clientId, Set<Seat> seats, Instant expiresAt) {
            Hold hold = new Hold(++nextId, clientId, seats, expiresAt);
            queue.add(hold);

            if (queue.peek() == hold) {
                notify();
            }

            return hold;
        }

        synchronized Hold next() throws InterruptedException {
            while (!stopped) {
                Hold head = queue.peek();
                Instant now = Instant.now();

                if (head == null) {
                    wait();
                } else if (now.isBefore(head.expiresAt())) {
                    wait(waitMillis(Duration.between(now, head.expiresAt())));
                } else {
                    return queue.poll();
                }
            }

            return null;
        }

        synchronized boolean stop() {
            if (stopped) {
                return false;
            }

            stopped = true;
            notify();
            return true;
        }
    }
}
