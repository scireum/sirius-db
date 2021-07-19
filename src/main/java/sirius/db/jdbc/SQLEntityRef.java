/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.io.Serial;
import java.util.Optional;

/**
 * Represents a reference from one entity to another.
 * <p>
 * Instead of directly keeping the entity in a Java field, it is wrapped in an <tt>EntityRef</tt>. This leads to clean
 * semantics for lazy loading as both the ID and (if fetched) the value are stored in this wrapper.
 *
 * @param <E> the generic type of the referenced entity
 */
public class SQLEntityRef<E extends SQLEntity> extends BaseEntityRef<Long, E> {

    @Serial
    private static final long serialVersionUID = 1326527428258744219L;

    @Part
    private static OMA oma;

    protected SQLEntityRef(Class<E> type, OnDelete deleteHandler, boolean writeOnce) {
        super(type, deleteHandler, writeOnce);
    }

    /**
     * Generates an entity reference to the given entity type.
     *
     * @param type          the target type to reference
     * @param deleteHandler determines what happens if the referenced entity is deleted
     * @param <E>           the generic type of the referenced entity
     * @return a new entity ref, representing the given settings
     */
    public static <E extends SQLEntity> SQLEntityRef<E> on(Class<E> type, OnDelete deleteHandler) {
        return new SQLEntityRef<>(type, deleteHandler, false);
    }

    /**
     * Generates an entity reference to the given entity type which has <b>write once semantics</b>.
     * <p>
     * A write once reference can only be set when the entity is new and never be changed afterwards.
     *
     * @param type          the target type to reference
     * @param deleteHandler determines what happens if the referenced entity is deleted
     * @param <E>           the generic type of the referenced entity
     * @return a new entity ref, representing the given settings
     * @see BaseEntityRef#hasWriteOnceSemantics()
     */
    public static <E extends SQLEntity> SQLEntityRef<E> writeOnceOn(Class<E> type, OnDelete deleteHandler) {
        return new SQLEntityRef<>(type, deleteHandler, true);
    }

    @Override
    protected Optional<E> find(Class<E> type, Long id) {
        return oma.find(type, id);
    }

    @Override
    protected Long coerceToId(Object id) {
        try {
            if (id instanceof Long) {
                return (Long) id;
            }
            return Long.parseLong(id.toString());
        } catch (NumberFormatException e) {
            Exceptions.ignore(e);
            return SQLEntity.NON_PERSISTENT_ENTITY_ID;
        }
    }
}
