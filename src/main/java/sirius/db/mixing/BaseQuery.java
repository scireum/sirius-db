/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mongo.Mongo;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.ValueHolder;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base class for queries within mixing.
 *
 * @param <E> the generic type of the entity being queried
 */
public abstract class BaseQuery<Q, E extends BaseEntity<?>> {

    public static final int MAX_LIST_SIZE = 1000;

    /**
     * Contains the max number of items to fetch (or 0 for "unlimited")
     */
    protected int limit;

    /**
     * Contains the the number of items to skip before items are added to the result
     */
    protected int skip;

    @Part
    protected static Mixing mixing;

    protected final EntityDescriptor descriptor;

    /**
     * Creates a new query for entities of the given type
     */
    protected BaseQuery(EntityDescriptor descriptor) {
        this.descriptor = descriptor;
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
    @SuppressWarnings("unchecked")
    public Q limit(int limit) {
        this.limit = limit;
        return (Q) this;
    }

    /**
     * Specifies the number of items to skip before items are added to the result.
     *
     * @param skip the number of items to skip. Value &lt;= 0 are ignored.
     * @return the query itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public Q skip(int skip) {
        this.skip = skip;
        return (Q) this;
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
        List<E> result = new ArrayList<>();

        // Ensure a sane limit...
        if (limit > MAX_LIST_SIZE) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage(
                                    "When using 'queryList' as most %s items can be selected. Use 'iterate' for larger results. Query: %s",
                                    MAX_LIST_SIZE,
                                    this)
                            .handle();
        }

        // Install circuit breaker...
        if (limit == 0) {
            limit = MAX_LIST_SIZE;
        }

        iterateAll(entity -> {
            result.add(entity);
            failIOnOverflow(result);
        });

        return result;
    }

    protected void failIOnOverflow(List<E> result) {
        if (result.size() >= MAX_LIST_SIZE) {
            throw Exceptions.handle()
                            .to(Mongo.LOG)
                            .withSystemErrorMessage(
                                    "More than %s results were loaded into a list by executing: %s",
                                    MAX_LIST_SIZE,
                                    this)
                            .handle();
        }
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
        limit(1);
        iterate(r -> {
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
        limit(2);
        List<E> result = queryList();
        if (result.size() != 1) {
            return null;
        } else {
            return result.get(0);
        }
    }
}
