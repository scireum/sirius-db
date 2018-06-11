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
import sirius.db.mixing.Query;
import sirius.kernel.di.std.Part;

import java.util.function.Function;

public class MongoQuery<E extends MongoEntity> extends Query<MongoQuery<E>, E> {

    private Finder finder;

    @Part
    private static Mango mango;

    @Part
    private static Mongo mongo;

    public MongoQuery(EntityDescriptor descriptor) {
        super(descriptor);
        this.finder = mongo.find();
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

    /**
     * Adds a complex filter which determines which documents should be selected.
     *
     * @param filter the filter to apply
     * @return the builder itself for fluent method calls
     */
    public MongoQuery<E> where(Filter filter) {
        finder.where(filter);
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

    public void delete() {
        iterateAll(mango::delete);
    }

    @Override
    public String toString() {
        return descriptor.getType() + ": " + finder.toString();
    }
}
