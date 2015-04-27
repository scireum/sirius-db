/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.ValueHolder;
import sirius.kernel.di.std.Part;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by aha on 26.04.15.
 */
abstract class BaseQuery<E extends Entity> {
    protected Class<E> type;
    protected int limit;
    protected int start;

    @Part
    private static Schema schema;

    public BaseQuery(Class<E> type) {
        this.type = type;
    }

    public BaseQuery<E> limit(int limit) {
        this.limit = limit;
        return this;
    }

    public BaseQuery<E> start(int start) {
        this.start = start;
        return this;
    }

    protected Limit getLimit() {
        return new Limit(start - 1, limit);
    }

    public List<E> queryList() {
        List<E> result = Lists.newArrayList();
        iterateAll(result::add);
        return result;
    }

    protected EntityDescriptor getDescriptor() {
        return schema.getDescriptor(type);
    }

    public abstract void iterate(Function<E, Boolean> handler);

    public void iterateAll(Consumer<E> consumer) {
        iterate(r -> {
            consumer.accept(r);
            return true;
        });
    }

    public Optional<E> first() {
        return Optional.ofNullable(queryFirst());
    }

    public E queryFirst() {
        ValueHolder<E> result = ValueHolder.of(null);
        limit(1).iterate(r -> {
            result.set(r);
            return false;
        });

        return result.get();
    }

    public Optional<E> one() {
        return Optional.ofNullable(queryOne());
    }

    public E queryOne() {
        List<E> result = limit(2).queryList();
        if (result.size() != 1) {
            return null;
        } else {
            return result.get(0);
        }
    }
}
