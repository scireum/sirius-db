/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.util;

import java.util.LongSummaryStatistics;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A stream wrapper calling {@link #close()} after any terminal operation.
 */
public final class AutoClosingLongStream implements LongStream {
    private final LongStream delegate;

    /**
     * Construct the stream wrapper
     */
    public AutoClosingLongStream(LongStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public LongStream filter(LongPredicate predicate) {
        return new AutoClosingLongStream(delegate.filter(predicate));
    }

    @Override
    public LongStream map(LongUnaryOperator mapper) {
        return new AutoClosingLongStream(delegate.map(mapper));
    }

    @Override
    public <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
        return new AutoClosingStream<>(delegate.mapToObj(mapper));
    }

    @Override
    public IntStream mapToInt(LongToIntFunction mapper) {
        return new AutoClosingIntStream(delegate.mapToInt(mapper));
    }

    @Override
    public DoubleStream mapToDouble(LongToDoubleFunction mapper) {
        return new AutoClosingDoubleStream(delegate.mapToDouble(mapper));
    }

    @Override
    public LongStream flatMap(LongFunction<? extends LongStream> mapper) {
        return new AutoClosingLongStream(delegate.flatMap(mapper));
    }

    @Override
    public LongStream distinct() {
        return new AutoClosingLongStream(delegate.distinct());
    }

    @Override
    public LongStream sorted() {
        return new AutoClosingLongStream(delegate.sorted());
    }

    @Override
    public LongStream peek(LongConsumer action) {
        return new AutoClosingLongStream(delegate.peek(action));
    }

    @Override
    public LongStream limit(long maxSize) {
        return new AutoClosingLongStream(delegate.limit(maxSize));
    }

    @Override
    public LongStream skip(long n) {
        return new AutoClosingLongStream(delegate.skip(n));
    }

    @Override
    public void forEach(LongConsumer action) {
        try (this) {
            delegate.forEach(action);
        }
    }

    @Override
    public void forEachOrdered(LongConsumer action) {
        try (this) {
            delegate.forEachOrdered(action);
        }
    }

    @Override
    public long[] toArray() {
        try (this) {
            return delegate.toArray();
        }
    }

    @Override
    public long reduce(long identity, LongBinaryOperator accumulator) {
        try (this) {
            return delegate.reduce(identity, accumulator);
        }
    }

    @Override
    public OptionalLong reduce(LongBinaryOperator accumulator) {
        try (this) {
            return delegate.reduce(accumulator);
        }
    }

    @Override
    public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        try (this) {
            return delegate.collect(supplier, accumulator, combiner);
        }
    }

    @Override
    public long sum() {
        try (this) {
            return delegate.sum();
        }
    }

    @Override
    public OptionalLong min() {
        try (this) {
            return delegate.min();
        }
    }

    @Override
    public OptionalLong max() {
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
    public LongSummaryStatistics summaryStatistics() {
        try (this) {
            return delegate.summaryStatistics();
        }
    }

    @Override
    public boolean anyMatch(LongPredicate predicate) {
        try (this) {
            return delegate.anyMatch(predicate);
        }
    }

    @Override
    public boolean allMatch(LongPredicate predicate) {
        try (this) {
            return delegate.allMatch(predicate);
        }
    }

    @Override
    public boolean noneMatch(LongPredicate predicate) {
        try (this) {
            return delegate.noneMatch(predicate);
        }
    }

    @Override
    public OptionalLong findFirst() {
        try (this) {
            return delegate.findFirst();
        }
    }

    @Override
    public OptionalLong findAny() {
        try (this) {
            return delegate.findAny();
        }
    }

    @Override
    public DoubleStream asDoubleStream() {
        return new AutoClosingDoubleStream(delegate.asDoubleStream());
    }

    @Override
    public Stream<Long> boxed() {
        return new AutoClosingStream<>(delegate.boxed());
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }

    @Override
    public LongStream sequential() {
        return new AutoClosingLongStream(delegate.sequential());
    }

    @Override
    public LongStream parallel() {
        return new AutoClosingLongStream(delegate.parallel());
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        return Spliterators.iterator(spliterator());
    }

    @Override
    public Spliterator.OfLong spliterator() {
        return Spliterators.spliterator(toArray(), Spliterator.IMMUTABLE | Spliterator.ORDERED);
    }

    @Override
    public LongStream unordered() {
        return new AutoClosingLongStream(delegate.unordered());
    }

    @Override
    public LongStream onClose(Runnable closeHandler) {
        return new AutoClosingLongStream(delegate.onClose(closeHandler));
    }

    @Override
    public void close() {
        delegate.close();
    }

    public LongStream delegate() {
        return delegate;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (AutoClosingLongStream) obj;
        return Objects.equals(this.delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }

    @Override
    public String toString() {
        return "AutoClosingLongStream[" + "delegate=" + delegate + ']';
    }
}
