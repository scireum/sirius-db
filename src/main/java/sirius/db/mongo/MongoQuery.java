/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.db.mongo.constraints.MongoConstraint;
import sirius.db.mongo.facets.MongoFacet;
import sirius.kernel.di.std.Part;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Creates a new query against MongoDB.
 *
 * @param <E> the type of entities being queried.
 */
public class MongoQuery<E extends MongoEntity> extends Query<MongoQuery<E>, E, MongoConstraint> {

    private Finder finder;

    private List<MongoFacet> facets;

    @Part
    private static Mango mango;

    @Part
    private static Mongo mongo;

    /**
     * Creates a new query for the given descriptor.
     *
     * @param descriptor the descriptor of the entities being queried
     */
    protected MongoQuery(EntityDescriptor descriptor) {
        super(descriptor);
        this.finder = mongo.find(descriptor.getRealm());
    }

    /**
     * Limits the fields being returned to the given list.
     *
     * @param fieldsToReturn specified the list of fields to return
     * @return the builder itself for fluent method calls
     */
    public MongoQuery<E> fields(Mapping... fieldsToReturn) {
        finder.selectFields(fieldsToReturn);

        return this;
    }

    @Override
    public MongoQuery<E> eq(Mapping key, Object value) {
        finder.where(key.toString(), value);
        return this;
    }

    @Override
    public MongoQuery<E> eqIgnoreNull(Mapping field, Object value) {
        if (value != null) {
            return eq(field, value);
        } else {
            return this;
        }
    }

    @Override
    public MongoQuery<E> where(MongoConstraint constraint) {
        if (constraint != null) {
            finder.where(constraint);
        }

        return this;
    }

    @Override
    public MongoQuery<E> orderAsc(Mapping field) {
        finder.orderByAsc(field.toString());
        return this;
    }

    @Override
    public MongoQuery<E> orderDesc(Mapping field) {
        finder.orderByDesc(field.toString());
        return this;
    }

    /**
     * Adds a limit to the query.
     *
     * @param skip  the number of items to skip (used for pagination).
     * @param limit the max. number of items to return (exluding those who have been skipped).
     * @return the builder itself for fluent method calls
     */
    public MongoQuery<E> limit(int skip, int limit) {
        finder.limit(skip, limit);

        return this;
    }

    @Override
    public MongoQuery<E> skip(int skip) {
        finder.skip(skip);
        return this;
    }

    @Override
    public MongoQuery<E> limit(int limit) {
        finder.limit(limit);

        return this;
    }

    @Override
    public void iterate(Function<E, Boolean> resultHandler) {
        finder.eachIn(descriptor.getRelationName(), doc -> resultHandler.apply(Mango.make(descriptor, doc)));
    }

    @Override
    public long count() {
        return finder.countIn(descriptor.getRelationName());
    }

    @Override
    public boolean exists() {
        return finder.selectFields(MongoEntity.ID).singleIn(descriptor.getRelationName()).isPresent();
    }

    @Override
    public void delete() {
        iterateAll(mango::delete);
    }

    @Override
    public void truncate() {
        Deleter deleter = mongo.delete();
        finder.transferFilters(deleter);
        deleter.manyFrom(descriptor.getRelationName());
    }

    @Override
    public FilterFactory<MongoConstraint> filters() {
        return QueryBuilder.FILTERS;
    }

    /**
     * Adds a facet to be later executed using {@link #executeFacets()}.
     *
     * @param facet the facet to add
     * @return the query itself for fluent method calls
     */
    public MongoQuery<E> addFacet(MongoFacet facet) {
        if (facets == null) {
            facets = new ArrayList<>();
        }

        facets.add(facet);
        return this;
    }

    /**
     * Executes all previously attached facets in one go.
     */
    public void executeFacets() {
        if (facets != null) {
            finder.executeFacets(descriptor, facets);
        }
    }

    /**
     * Returns an <tt>$explain</tt> for this query.
     *
     * @return the explanation delivered by MongoDB
     */
    public Doc explain() {
        return finder.explain(descriptor.getRelationName());
    }

    @Override
    public String toString() {
        return descriptor.getType() + ": " + finder.toString();
    }
}
