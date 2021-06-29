/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.util;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A stream wrapper calling {@link #close()} after any terminal operation.
 */
public record AutoClosingIntStream(IntStream delegate) implements IntStream {

    @Override
    public IntStream filter(IntPredicate predicate) {
        return new AutoClosingIntStream(delegate.filter(predicate));
    }

    @Override
    public IntStream map(IntUnaryOperator mapper) {
        return new AutoClosingIntStream(delegate.map(mapper));
    }

    @Override
    public <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
        return new AutoClosingStream<>(delegate.mapToObj(mapper));
    }

    @Override
    public LongStream mapToLong(IntToLongFunction mapper) {
        return new AutoClosingLongStream(delegate.mapToLong(mapper));
    }

    @Override
    public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
        return new AutoClosingDoubleStream(delegate.mapToDouble(mapper));
    }

    @Override
    public IntStream flatMap(IntFunction<? extends IntStream> mapper) {
        return new AutoClosingIntStream(delegate.flatMap(mapper));
    }

    @Override
    public IntStream distinct() {
        return new AutoClosingIntStream(delegate.distinct());
    }

    @Override
    public IntStream sorted() {
        return new AutoClosingIntStream(delegate.sorted());
    }

    @Override
    public IntStream peek(IntConsumer action) {
        return new AutoClosingIntStream(delegate.peek(action));
    }

    @Override
    public IntStream limit(long maxSize) {
        return new AutoClosingIntStream(delegate.limit(maxSize));
    }

    @Override
    public IntStream skip(long n) {
        return new AutoClosingIntStream(delegate.skip(n));
    }

    @Override
    public void forEach(IntConsumer action) {
        try (this) {
            delegate.forEach(action);
        }
    }

    @Override
    public void forEachOrdered(IntConsumer action) {
        try (this) {
            delegate.forEachOrdered(action);
        }
    }

    @Override
    public int[] toArray() {
        try (this) {
            return delegate.toArray();
        }
    }

    @Override
    public int reduce(int identity, IntBinaryOperator accumulator) {
        try (this) {
            return delegate.reduce(identity, accumulator);
        }
    }

    @Override
    public OptionalInt reduce(IntBinaryOperator accumulator) {
        try (this) {
            return delegate.reduce(accumulator);
        }
    }

    @Override
    public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        try (this) {
            return delegate.collect(supplier, accumulator, combiner);
        }
    }

    @Override
    public int sum() {
        try (this) {
            return delegate.sum();
        }
    }

    @Override
    public OptionalInt min() {
        try (this) {
            return delegate.min();
        }
    }

    @Override
    public OptionalInt max() {
        try (this) {
            return delegate.max();
        }
    }

    @Override
    public long count() {
        try (this) {
            return delegate.count();
        }
    }

    @Override
    public OptionalDouble average() {
        try (this) {
            return delegate.average();
        }
    }

    @Override
    public IntSummaryStatistics summaryStatistics() {
        try (this) {
            return delegate.summaryStatistics();
        }
    }

    @Override
    public boolean anyMatch(IntPredicate predicate) {
        try (this) {
            return delegate.anyMatch(predicate);
        }
    }

    @Override
    public boolean allMatch(IntPredicate predicate) {
        try (this) {
            return delegate.allMatch(predicate);
        }
    }

    @Override
    public boolean noneMatch(IntPredicate predicate) {
        try (this) {
            return delegate.noneMatch(predicate);
        }
    }

    @Override
    public OptionalInt findFirst() {
        try (this) {
            return delegate.findFirst();
        }
    }

    @Override
    public OptionalInt findAny() {
        try (this) {
            return delegate.findAny();
        }
    }

    @Override
    public LongStream asLongStream() {
        return new AutoClosingLongStream(delegate.asLongStream());
    }

    @Override
    public DoubleStream asDoubleStream() {
        return new AutoClosingDoubleStream(delegate.asDoubleStream());
    }

    @Override
    public Stream<Integer> boxed() {
        return new AutoClosingStream<>(delegate.boxed());
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }

    @Override
    public IntStream sequential() {
        return new AutoClosingIntStream(delegate.sequential());
    }

    @Override
    public IntStream parallel() {
        return new AutoClosingIntStream(delegate.parallel());
    }

    @Override
    public PrimitiveIterator.OfInt iterator() {
        return Spliterators.iterator(spliterator());
    }

    @Override
    public Spliterator.OfInt spliterator() {
        return Spliterators.spliterator(toArray(), Spliterator.IMMUTABLE | Spliterator.ORDERED);
    }

    @Override
    public IntStream unordered() {
        return new AutoClosingIntStream(delegate.unordered());
    }

    @Override
    public IntStream onClose(Runnable closeHandler) {
        return new AutoClosingIntStream(delegate.onClose(closeHandler));
    }

    @Override
    public void close() {
        delegate.close();
    }
}
