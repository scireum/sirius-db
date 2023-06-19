/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.types;

import sirius.db.es.Elastic;
import sirius.db.es.ElasticEntity;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.di.std.Part;

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
public class ElasticRef<E extends ElasticEntity> extends BaseEntityRef<String, E> {

    @Serial
    private static final long serialVersionUID = -2145898365724128406L;

    @Part
    private static Elastic elastic;

    protected ElasticRef(Class<E> type, OnDelete deleteHandler, boolean writeOnce) {
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
    public static <E extends ElasticEntity> ElasticRef<E> on(Class<E> type, OnDelete deleteHandler) {
        return new ElasticRef<>(type, deleteHandler, false);
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
    public static <E extends ElasticEntity> ElasticRef<E> writeOnceOn(Class<E> type, OnDelete deleteHandler) {
        return new ElasticRef<>(type, deleteHandler, true);
    }

    @Override
    protected Optional<E> find(Class<E> type, String id) {
        return elastic.find(type, id);
    }

    @Override
    protected Optional<E> findInSecondary(Class<E> type, String id) {
        return find(type, id);
    }

    @Override
    protected String coerceToId(Object id) {
        return id.toString();
    }
}
