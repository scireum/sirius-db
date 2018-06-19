/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.types;

import sirius.db.mixing.ContextInfo;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.db.mixing.types.BaseEntityRefList;
import sirius.db.mongo.Mango;
import sirius.db.mongo.MongoEntity;
import sirius.kernel.di.std.Part;

import java.util.Optional;

/**
 * Represents a list of {@link MongoEntity entities} being referenced by id.
 *
 * @param <E> the type of entities being referenced
 */
public class MongoRefList<E extends MongoEntity> extends BaseEntityRefList<E, MongoRefList<E>> {

    @Part
    private static Mango mango;

    /**
     * Creates a new list for the given type and delete handler.
     *
     * @param type          the type of entities to store in the list
     * @param deleteHandler the behaviour when one of the stored entities is deleted. Note that
     *                      {@link sirius.db.mixing.types.BaseEntityRef.OnDelete#SET_NULL} will remove the id from the
     *                      list instead of inserting a <tt>null</tt>.
     */
    public MongoRefList(Class<E> type, BaseEntityRef.OnDelete deleteHandler) {
        super(type, deleteHandler);
    }

    @Override
    protected Optional<E> resolve(String id, ContextInfo... context) {
        return mango.find(type, id);
    }
}
