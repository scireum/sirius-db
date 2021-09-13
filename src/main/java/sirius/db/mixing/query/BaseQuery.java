/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.query;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.ValueHolder;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Base class for queries within mixing.
 * <p>
 * Note that next to obtaining single row results our checking the existence or an entity or performing counts,
 * this provides three main APIs of obtaining multiple entities.
 * <p>
 * <ul>
 * <li>For smaller result sets (less than 1000 results), use {@link #queryList()} or {@link #streamList()}</li>
 * <li>
 *     For medium sized results, which can be processed quickly, without hitting any database timeouts
 *     (e.g. in less than one hour) use {@link #iterate(Predicate)} or {@link #iterateAll(Consumer)}.
 * </li>
 * <li>
 *     For very large result sets use {@link #streamBlockwise()}, but be aware, that this might come with less
 *     quarantees, as the result is fetched and processed in blocks (hence its name).
 * </li>
 * </ul>
 *
 * @param <Q> the generic parameter of the effective query class
 * @param <E> the generic type of the entity being queried
 */
public abstract class BaseQuery<Q, E extends BaseEntity<?>> {

    /**
     * Contains the maximal number of elements to be returned in {@link #queryList()}.
     * <p>
     * For larger results {@link #iterate(Predicate)} mit be used as it can be more efficient
     */
    public static final int MAX_LIST_SIZE = 1000;

    /**
     * Contains the max number of items to fetch (or 0 for "unlimited")
     */
    protected int limit;

    /**
     * Contains the the number of items to skip before items are added to the result
     */
    protected int skip;

    /**
     * If true, the query is marked as failed and should always return an empty result.
     */
    protected boolean forceFail;

    @Part
    protected static Mixing mixing;

    /**
     * Contains the descriptor of the entities being queried.
     */
    protected final EntityDescriptor descriptor;

    /**
     * Creates a new query for entities of the given type.
     *
     * @param descriptor the descriptor of the entity type to query
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

    public boolean isForceFail() {
        return forceFail;
    }

    /**
     * Returns the underlying descriptor
     *
     * @return the descriptor of the entities being queried.
     */
    public EntityDescriptor getDescriptor() {
        return descriptor;
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
        this.skip = skip > 0 ? skip : this.skip;
        return (Q) this;
    }

    /**
     * Marks this query as failed or invalid. Therefore, no matter on what constraints are set, this query will always
     * return an empty result.
     * <p>
     * This method is intended for security checks which should not abort processing but just behave like the
     * query didn't match any entities.
     *
     * @return the query itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public Q fail() {
        forceFail = true;
        return (Q) this;
    }

    /**
     * Returns a list of all items in the result.
     * <p>
     * Note that this method must only be used for results with a known size which is smaller than
     * {@link #MAX_LIST_SIZE}. Larger results should be processed using {@link #iterate(Predicate)} or
     * {@link #iterateAll(Consumer)} or even with {@link #streamBlockwise()} which is virtually capable or processing
     * results of any size.
     *
     * @return a list of items in the query or an empty list if the query did not match any items
     */
    public List<E> queryList() {
        List<E> result = new ArrayList<>();
        if (forceFail) {
            return result;
        }

        // Ensure a sane limit...
        if (limit > MAX_LIST_SIZE) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("A limit of %s items can be selected when using 'queryList'. "
                                                    + "Use 'iterate' for larger results. Query: %s",
                                                    MAX_LIST_SIZE,
                                                    this)
                            .handle();
        }

        // Install circuit breaker...
        int originalLimit = limit;
        if (limit == 0) {
            limit = MAX_LIST_SIZE + 1;
        }

        iterateAll(entity -> {
            result.add(entity);
            failOnOverflow(result);
        });

        // Restore limit in case the code above changed it.
        limit = originalLimit;

        return result;
    }

    /**
     * Returns a stream containing of all items in the result.
     * <p>
     * Note that this method must only be used for results with a known size which is smaller than
     * {@link #MAX_LIST_SIZE}. Larger results should be processed using {@link #iterate(Predicate)} or
     * {@link #iterateAll(Consumer)} or even with {@link #streamBlockwise()} which is virtually capable or processing
     * results of any size.
     *
     * @return a stream of items in the query
     */
    public Stream<E> streamList() {
        return queryList().stream();
    }

    /**
     * Calls the given function on all items in the result, as long as it returns <tt>true</tt>.
     * <p>
     * Note that this method is intended for medium-sized results as not all items in the result need to be
     * kept in memory when iterating through them. Note however, that for verly large result sets, a method
     * like {@link #streamBlockwise()} might be more appropriate, as it ensures that underlying resources
     * like <tt>cursors</tt> or <tt>database connections</tt> cannot run into a timeout.
     *
     * @param handler the handler to be invoked for each item in the result. Should return <tt>true</tt>
     *                to continue processing or <tt>false</tt> to abort processing of the result set.
     */
    public abstract void iterate(Predicate<E> handler);

    /**
     * Calls the given consumer on all items in the result.
     * <p>
     * Note that this method is intended for medium-sized results just like {@link #iterate(Predicate)}.
     *
     * @param consumer the handler to be invoked for each item in the result
     */
    public void iterateAll(Consumer<E> consumer) {
        iterate(r -> {
            consumer.accept(r);
            return true;
        });
    }

    /**
     * Fetches the effective result in a blockwise manner.
     * <p>
     * Adapts the underlying query, so that the underlying database only fetches and provides a block of matching
     * entities. Once these are processed, the next block will be fetched.
     * <p>
     * This has the benefit, that results of virtually any size can be processed in a safe manner, without locking
     * any database resources and also without the risk of running into a timout of an underlying resource.
     * <p>
     * While we try hard to keep the result consistent, there is no way to guarantee this. There is a possibility,
     * that we either miss an entity or even process an entity twice if a concurrent modification happens. Basically,
     * you might get <a href="https://en.wikipedia.org/wiki/Isolation_(database_systems)#Non-repeatable_reads">
     * non-repeatable reads</a> or <a href="https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads">
     * phantom reads</a>.
     *
     * @return the stream of matched entities
     */
    public abstract Stream<E> streamBlockwise();

    protected void failOnOverflow(List<E> result) {
        if (result.size() > MAX_LIST_SIZE) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("More than %s results were loaded into a list by executing: %s",
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
