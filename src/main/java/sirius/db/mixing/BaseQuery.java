/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.ValueHolder;
import sirius.kernel.di.std.Part;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base class for queries within mixing.
 *
 * @param <E> the generic type of the entity being queried
 */
abstract class BaseQuery<E extends Entity> {
    /*
     * Contains the entity type which are to be queried
     */
    protected Class<E> type;

    /*
     * Contains the max number of items to fetch (or 0 for "unlimited")
     */
    protected int limit;

    /*
     * Contains the the number of items to skip before items are added to the result
     */
    protected int skip;

    @Part
    private static Schema schema;

    /*
     * Creates a new query for entities of the given type
     */
    BaseQuery(Class<E> type) {
        this.type = type;
    }

    /*
     * Returns the descriptor of the entity type of the query.
     */
    protected EntityDescriptor getDescriptor() {
        return schema.getDescriptor(type);
    }

    /*
     * Returns skip and limit as Limit object
     */
    protected Limit getLimit() {
        return new Limit(skip, limit);
    }

    /**
     * Specifies the max. number of items to fetch.
     *
     * @param limit the max. number of items to fetch. Value &lt;= 0 indicate "unlimited".
     * @return the query itself for fluent method calls
     */
    public BaseQuery<E> limit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Specifies the number of items to skip before items are added to the result.
     *
     * @param skip the number of items to skip. Value &lt;= 0 are ignored.
     * @return the query itself for fluent method calls
     */
    public BaseQuery<E> skip(int skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Calls the given function on all items in the result, as long as it returns <tt>true</tt>.
     * <p>
     * Note that this method is intended for large results as not all items in the result need to be
     * kept in memory when iterating through them.
     *
     * @param handler the handle to be invoked for each item in the result
     */
    public abstract void iterate(Function<E, Boolean> handler);

    /**
     * Calls the given consumer on all items in the result.
     * <p>
     * Note that this method is intended for large results as not all items in the result need to be
     * kept in memory when iterating through them.
     *
     * @param consumer the handle to be invoked for each item in the result
     */
    public void iterateAll(Consumer<E> consumer) {
        iterate(r -> {
            consumer.accept(r);
            return true;
        });
    }

    /**
     * Returns a list of all items in the result.
     * <p>
     * Note that large results should be processed using {@link #iterate(Function)} or
     * {@link #iterateAll(Consumer)} as they are more memory efficient.
     *
     * @return a list of items in the query or an empty list if the query did not match any items
     */
    public List<E> queryList() {
        List<E> result = Lists.newArrayList();
        iterateAll(result::add);
        return result;
    }

    /**
     * Returns the first item matched by the query.
     *
     * @return the first item matched by the query wrapped as <tt>Optional</tt>. Returns an empty optional, if the
     * query has no matches.
     */
    public Optional<E> first() {
        return Optional.ofNullable(queryFirst());
    }

    /**
     * Returns the first item matched by the query.
     *
     * @return the first item matched by the query or <tt>null</tt> if there are no matches.
     */
    public E queryFirst() {
        ValueHolder<E> result = ValueHolder.of(null);
        limit(1).iterate(r -> {
            result.set(r);
            return false;
        });

        return result.get();
    }

    /**
     * Returns the single item matched by the query.
     *
     * @return the first item matched by the query wrapped as <tt>Optional</tt>. Returns an empty optional, if the
     * query has no <b>or several</b> matches.
     */
    public Optional<E> one() {
        return Optional.ofNullable(queryOne());
    }

    /**
     * Returns the first item matched by the query.
     *
     * @return the first item matched by the query or <tt>null</tt> if the query has no <b>or several</b> matches.
     */
    public E queryOne() {
        List<E> result = limit(2).queryList();
        if (result.size() != 1) {
            return null;
        } else {
            return result.get(0);
        }
    }
}
