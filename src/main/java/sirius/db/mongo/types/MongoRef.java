/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.types;

import sirius.db.mixing.types.BaseEntityRef;
import sirius.db.mongo.Mango;
import sirius.db.mongo.MongoEntity;
import sirius.kernel.di.std.Part;

import java.util.Optional;

/**
 * Represents a reference from one entity to another.
 * <p>
 * Instead of directly keeping the entity in a Java field, it is wrapped in an <tt>EntityRef</tt>. This leads to clean
 * semantics for lazy loading as both the ID and (if fetched) the value are stored in this wrapper.
 *
 * @param <E> the generic type of the referenced entity
 */
public class MongoRef<E extends MongoEntity> extends BaseEntityRef<String, E> {

    @Part
    private static Mango mango;

    protected MongoRef(Class<E> type, OnDelete deleteHandler) {
        super(type, deleteHandler);
    }

    /**
     * Generates an entity reference to the given entity type.
     *
     * @param type          the target type to reference
     * @param deleteHandler determines what happens if the referenced entity is deleted
     * @param <E>           the generic type of the referenced entity
     * @return a new entity ref, representing the given settings
     */
    public static <E extends MongoEntity> MongoRef<E> on(Class<E> type, OnDelete deleteHandler) {
        return new MongoRef<>(type, deleteHandler);
    }

    @Override
    protected Optional<E> find(Class<E> type, String id) {
        return mango.find(type, id);
    }
}
