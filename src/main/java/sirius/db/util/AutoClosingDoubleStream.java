/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.util;

import java.util.DoubleSummaryStatistics;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A stream wrapper calling {@link #close()} after any terminal operation.
 */
public final class AutoClosingDoubleStream implements DoubleStream {
    private final DoubleStream delegate;

    /**
     * Construct the stream wrapper
     */
    public AutoClosingDoubleStream(DoubleStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public DoubleStream filter(DoublePredicate predicate) {
        return new AutoClosingDoubleStream(delegate.filter(predicate));
    }

    @Override
    public DoubleStream map(DoubleUnaryOperator mapper) {
        return new AutoClosingDoubleStream(delegate.map(mapper));
    }

    @Override
    public <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
        return new AutoClosingStream<>(delegate.mapToObj(mapper));
    }

    @Override
    public IntStream mapToInt(DoubleToIntFunction mapper) {
        return new AutoClosingIntStream(delegate.mapToInt(mapper));
    }

    @Override
    public LongStream mapToLong(DoubleToLongFunction mapper) {
        return new AutoClosingLongStream(delegate.mapToLong(mapper));
    }

    @Override
    public DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
        return new AutoClosingDoubleStream(delegate.flatMap(mapper));
    }

    @Override
    public DoubleStream distinct() {
        return new AutoClosingDoubleStream(delegate.distinct());
    }

    @Override
    public DoubleStream sorted() {
        return new AutoClosingDoubleStream(delegate.sorted());
    }

    @Override
    public DoubleStream peek(DoubleConsumer action) {
        return new AutoClosingDoubleStream(delegate.peek(action));
    }

    @Override
    public DoubleStream limit(long maxSize) {
        return new AutoClosingDoubleStream(delegate.limit(maxSize));
    }

    @Override
    public DoubleStream skip(long n) {
        return new AutoClosingDoubleStream(delegate.skip(n));
    }

    @Override
    public void forEach(DoubleConsumer action) {
        try (this) {
            delegate.forEach(action);
        }
    }

    @Override
    public void forEachOrdered(DoubleConsumer action) {
        try (this) {
            delegate.forEachOrdered(action);
        }
    }

    @Override
    public double[] toArray() {
        try (this) {
            return delegate.toArray();
        }
    }

    @Override
    public double reduce(double identity, DoubleBinaryOperator accumulator) {
        try (this) {
            return delegate.reduce(identity, accumulator);
        }
    }

    @Override
    public OptionalDouble reduce(DoubleBinaryOperator accumulator) {
        try (this) {
            return delegate.reduce(accumulator);
        }
    }

    @Override
    public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        try (this) {
            return delegate.collect(supplier, accumulator, combiner);
        }
    }

    @Override
    public double sum() {
        try (this) {
            return delegate.sum();
        }
    }

    @Override
    public OptionalDouble min() {
        try (this) {
            return delegate.min();
        }
    }

    @Override
    public OptionalDouble max() {
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
    public DoubleSummaryStatistics summaryStatistics() {
        try (this) {
            return delegate.summaryStatistics();
        }
    }

    @Override
    public boolean anyMatch(DoublePredicate predicate) {
        try (this) {
            return delegate.anyMatch(predicate);
        }
    }

    @Override
    public boolean allMatch(DoublePredicate predicate) {
        try (this) {
            return delegate.allMatch(predicate);
        }
    }

    @Override
    public boolean noneMatch(DoublePredicate predicate) {
        try (this) {
            return delegate.noneMatch(predicate);
        }
    }

    @Override
    public OptionalDouble findFirst() {
        try (this) {
            return delegate.findFirst();
        }
    }

    @Override
    public OptionalDouble findAny() {
        try (this) {
            return delegate.findAny();
        }
    }

    @Override
    public Stream<Double> boxed() {
        return new AutoClosingStream<>(delegate.boxed());
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }

    @Override
    public DoubleStream sequential() {
        return new AutoClosingDoubleStream(delegate.sequential());
    }

    @Override
    public DoubleStream parallel() {
        return new AutoClosingDoubleStream(delegate.parallel());
    }

    @Override
    public PrimitiveIterator.OfDouble iterator() {
        return Spliterators.iterator(spliterator());
    }

    @Override
    public Spliterator.OfDouble spliterator() {
        return Spliterators.spliterator(toArray(), Spliterator.IMMUTABLE | Spliterator.ORDERED);
    }

    @Override
    public DoubleStream unordered() {
        return new AutoClosingDoubleStream(delegate.unordered());
    }

    @Override
    public DoubleStream onClose(Runnable closeHandler) {
        return new AutoClosingDoubleStream(delegate.onClose(closeHandler));
    }

    @Override
    public void close() {
        delegate.close();
    }

    public DoubleStream delegate() {
        return delegate;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (AutoClosingDoubleStream) obj;
        return Objects.equals(this.delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }

    @Override
    public String toString() {
        return "AutoClosingDoubleStream[" + "delegate=" + delegate + ']';
    }
}
