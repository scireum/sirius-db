/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.util;

import sirius.kernel.commons.Explain;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A stream wrapper calling {@link #close()} after any terminal operation.
 *
 * @param <T> the type of the stream elements
 */
public final class AutoClosingStream<T> implements Stream<T> {
    private final Stream<T> delegate;

    /**
     * Construct the stream wrapper
     */
    public AutoClosingStream(Stream<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        return new AutoClosingStream<>(delegate.filter(predicate));
    }

    @Override
    public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
        return new AutoClosingStream<>(delegate.map(mapper));
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return new AutoClosingIntStream(delegate.mapToInt(mapper));
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return new AutoClosingLongStream(delegate.mapToLong(mapper));
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return new AutoClosingDoubleStream(delegate.mapToDouble(mapper));
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new AutoClosingStream<>(delegate.flatMap(mapper));
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return new AutoClosingIntStream(delegate.flatMapToInt(mapper));
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return new AutoClosingLongStream(delegate.flatMapToLong(mapper));
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return new AutoClosingDoubleStream(delegate.flatMapToDouble(mapper));
    }

    @Override
    public Stream<T> distinct() {
        return new AutoClosingStream<>(delegate.distinct());
    }

    @Override
    public Stream<T> sorted() {
        return new AutoClosingStream<>(delegate.sorted());
    }

    @Override
    public Stream<T> sorted(Comparator<? super T> comparator) {
        return new AutoClosingStream<>(delegate.sorted(comparator));
    }

    @Override
    public Stream<T> peek(Consumer<? super T> action) {
        return new AutoClosingStream<>(delegate.peek(action));
    }

    @Override
    public Stream<T> limit(long maxSize) {
        return new AutoClosingStream<>(delegate.limit(maxSize));
    }

    @Override
    public Stream<T> skip(long n) {
        return new AutoClosingStream<>(delegate.skip(n));
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        try (this) {
            delegate.forEach(action);
        }
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        try (this) {
            delegate.forEachOrdered(action);
        }
    }

    @Override
    public Object[] toArray() {
        try (this) {
            return delegate.toArray();
        }
    }

    @SuppressWarnings("SuspiciousToArrayCall")
    @Explain("""
            This behavior is intended by the Stream API:
            `Stream.of(1, 2, 3).toArray(String[]::new)` will compile, but throw a RuntimeException.
            This design choice is (probably) due to limitations in the generic type system, which does not permit
            `<A extends B>`, while `Stream.of(1, 2, 3).toArray(Number[]::new)` should be a legal call.""")
    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        try (this) {
            A[] x = generator.apply(1);
            return delegate.toArray(generator);
        }
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        try (this) {
            return delegate.reduce(identity, accumulator);
        }
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        try (this) {
            return delegate.reduce(accumulator);
        }
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        try (this) {
            return delegate.reduce(identity, accumulator, combiner);
        }
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        try (this) {
            return delegate.collect(supplier, accumulator, combiner);
        }
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        try (this) {
            return delegate.collect(collector);
        }
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        try (this) {
            return delegate.min(comparator);
        }
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        try (this) {
            return delegate.max(comparator);
        }
    }

    @Override
    public long count() {
        try (this) {
            return delegate.count();
        }
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        try (this) {
            return delegate.anyMatch(predicate);
        }
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        try (this) {
            return delegate.allMatch(predicate);
        }
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        try (this) {
            return delegate.noneMatch(predicate);
        }
    }

    @Override
    public Optional<T> findFirst() {
        try (this) {
            return delegate.findFirst();
        }
    }

    @Override
    public Optional<T> findAny() {
        try (this) {
            return delegate.findAny();
        }
    }

    /**
     * @implNote Because we need to guarantee that {@link #close()} is called, the result is collected into a list, and
     * iterated afterwards. Please avoid using this iterator.
     */
    @Override
    public Iterator<T> iterator() {
        return collect(Collectors.toList()).iterator();
    }

    /**
     * @implNote Because we need to guarantee that {@link #close()} is called, the result is collected into a list, and
     * iterated afterwards. Please avoid using this iterator.
     */
    @Override
    public Spliterator<T> spliterator() {
        return collect(Collectors.toList()).spliterator();
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }

    @Override
    public Stream<T> sequential() {
        return new AutoClosingStream<>(delegate.sequential());
    }

    @Override
    public Stream<T> parallel() {
        return new AutoClosingStream<>(delegate.parallel());
    }

    @Override
    public Stream<T> unordered() {
        return new AutoClosingStream<>(delegate.unordered());
    }

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
        return new AutoClosingStream<>(delegate.onClose(closeHandler));
    }

    @Override
    public void close() {
        delegate.close();
    }
}
