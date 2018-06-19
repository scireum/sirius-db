/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.ContextInfo;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Represents a list of IDs (strings) which represents subclasses of {@link sirius.db.mixing.BaseEntity}.
 * <p>
 * The main benefit over simply using a plain {@link StringList} is the referential integrity provided via the given
 * {@link sirius.db.mixing.types.BaseEntityRef.OnDelete delete handler}.
 *
 * @param <E> the type of entities being stored in this list
 * @param <L> the effective subclass used as fluent return type
 */
public abstract class BaseEntityRefList<E extends BaseEntity<String>, L extends BaseEntityRefList<E, L>>
        extends SafeList<String> {

    protected BaseEntityRef.OnDelete deleteHandler;
    protected Class<E> type;

    protected BaseEntityRefList(Class<E> type, BaseEntityRef.OnDelete deleteHandler) {
        this.type = type;
        this.deleteHandler = deleteHandler;
    }

    /**
     * Resolves a given ID into an entity instance.
     *
     * @param id      the id to resolve
     * @param context the context used for resolving (routing etc.)
     * @return the resolved entity wrapped as optional or an empty optional of the entity doesn't exist
     */
    protected abstract Optional<E> resolve(String id, ContextInfo... context);

    /**
     * Adds the given entity to the list.
     * <p>
     * If the entity is <tt>null</tt> or new, nothing will happen.
     *
     * @param entity the entity to add
     * @return the list itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public L add(E entity) {
        if (entity != null && !entity.isNew()) {
            add(entity.getId());
        }
        return (L) this;
    }

    /**
     * Removes the given entity from the list.
     * <p>
     * If the entity is <tt>null</tt> or new, nothing will happen.
     *
     * @param entity the entity to remove
     * @return the list itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public L remove(E entity) {
        if (entity != null && !entity.isNew()) {
            modify().remove(entity.getId());
        }
        return (L) this;
    }

    /**
     * Determines if the given entity is in the list.
     *
     * @param entity the entity to check for.
     * @return <tt>true</tt> if the entity is in the list, <tt>false</tt> if the entity was new,
     * <tt>null</tt> or not in the list
     */
    public boolean contains(E entity) {
        if (entity != null && !entity.isNew()) {
            return contains(entity.getId());
        }

        return false;
    }

    /**
     * Retruns all entity in the list by resolving them against the database.
     * <p>
     * This will perform N uncached lookups against the database - use with caution.
     *
     * @param context the lookup context
     * @return a stream of all entities in the list, wrapped as optional. May contain empty optionals for stale IDs
     */
    public Stream<Optional<E>> fetchAll(ContextInfo... context) {
        return data().stream().map(id -> resolve(id, context));
    }

    /**
     * Retruns all entity in the list by resolving them against the database.
     * <p>
     * This will perform N uncached lookups against the database - use with caution.
     *
     * @param context the lookup context
     * @return a stream of all entities in the list which also exist in the database
     */
    public Stream<E> fetchAllAvailable(ContextInfo... context) {
        return fetchAll(context).filter(Optional::isPresent).map(Optional::get);
    }

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected String copyValue(String value) {
        return value;
    }

    /**
     * Returns the delete handler to use.
     *
     * @return the delete handler to enforce for the list of IDs
     */
    public BaseEntityRef.OnDelete getDeleteHandler() {
        return deleteHandler;
    }

    /**
     * Returns the underlying entity type.
     *
     * @return the type of entities being stored / represented by this list
     */
    public Class<E> getType() {
        return type;
    }
}
