/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.Mixing;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a reference from one entity to another.
 * <p>
 * Instead of directly keeping the entity in a Java field, it is wrapped in an <tt>EntityRef</tt>. This leads to clean
 * semantics for lazy loading as both the ID and (if fetched) the value are stored in this wrapper.
 * <p>
 * Note that a reference is serializable but will only serialize the id, not the currently loaded value.
 *
 * @param <I> the generic type of the primary key / id used by the entity
 * @param <E> the generic type of the referenced entity
 */
public abstract class BaseEntityRef<I extends Serializable, E extends BaseEntity<I>> implements Serializable {

    @Serial
    private static final long serialVersionUID = -6950811329430896367L;

    /**
     * Declares cascade actions which determine what happens if a referenced entity is deleted.
     */
    public enum OnDelete {
        /**
         * Also delete the entity which references the entity being deleted.
         */
        CASCADE,

        /**
         * Set the entity reference to <tt>null</tt> once an entity is deleted.
         */
        SET_NULL,

        /**
         * Reject the delete.
         */
        REJECT,

        /**
         * No further checks are performed. If the referenced entity is deleted, nothing happens.
         */
        IGNORE
    }

    protected Class<E> type;
    protected transient OnDelete deleteHandler;
    protected transient boolean writeOnce;
    protected I id;
    protected transient E value;

    @Part
    private static Mixing mixing;

    protected BaseEntityRef(Class<E> type, OnDelete deleteHandler, boolean writeOnce) {
        this.type = type;
        this.deleteHandler = deleteHandler;
        this.writeOnce = writeOnce;
    }

    /**
     * Returns the entity type being referenced.
     *
     * @return the entity type being referenced
     */
    public Class<E> getType() {
        return type;
    }

    /**
     * Returns the delete handler.
     *
     * @return the handler which deciedes how to tread deletes of the referenced entity
     */
    public OnDelete getDeleteHandler() {
        return deleteHandler;
    }

    /**
     * Determines if this reference has a <b>write once</b> semantic.
     * <p>
     * A write once field can only be filled once when the entity is created but not changed afterwards. This of
     * an <tt>item</tt> within an <tt>invoice</tt>. The <b>invoice</b> reference would have a write once semantic
     * as an item of an invoice is never moved to another invoice.
     *
     * @return <tt>true</tt> if the reference has write once semantics, <tt>false</tt> otherwise
     */
    public boolean hasWriteOnceSemantics() {
        return writeOnce;
    }

    /**
     * Returns the id of the referenced entity.
     *
     * @return the id of the referenced entity or <tt>null</tt> if no entity is referenced.
     */
    @Nullable
    public I getId() {
        return id;
    }

    /**
     * Sets the id of the referenced entity.
     *
     * @param id the id of the referenced entity or <tt>null</tt> if no entity is referenced.
     */
    public void setId(@Nullable I id) {
        this.id = id;

        // As SQL entities use -1 to indicate that no id is available,
        // we handle this case gracefully here...
        if (this.id instanceof Long number && number < 0) {
            this.id = null;
        }

        if (value != null && (this.id == null || !this.id.equals(value.getId()))) {
            this.value = null;
        }
    }

    /**
     * Returns a string representation of the entity ID.
     *
     * @return the id of the referenced entity as String or <tt>null</tt> if no entity is referenced.
     */
    @Nullable
    public String getIdAsString() {
        return id == null ? null : String.valueOf(id);
    }

    /**
     * Returns the <tt>Unique Object Name</tt> for the referenced entity.
     *
     * @return the unique object name of the referenced entity or <tt>null</tt> if the reference is empty.
     * @see SQLEntity#getUniqueName()
     */
    public String getUniqueObjectName() {
        if (id == null) {
            return null;
        }

        return Mixing.getUniqueName(type, id);
    }

    /**
     * Returns the effective entity object which is referenced.
     * <p>
     * Note, this might cause a database lookup if the entity is not prefetched.
     *
     * @return the entity being referenced or <tt>null</tt> if no entity is referenced.
     */
    public E fetchValue() {
        if (value == null && id != null) {
            Optional<E> entity = find(type, id);
            if (entity.isPresent()) {
                value = entity.get();
            } else {
                id = null;
            }
        }
        return value;
    }

    /**
     * Returns the effective entity object which has been referenced, if it was loaded from the database.
     * <p>
     * Note that this will not perform a database lookup but only return the entity if is was already
     * loaded from somewhere else. Note that therefore an empty reference and a reference with no value
     * loaded cannot be distinguished.
     *
     * @return the referenced entity wrapped as optional or an empty optional if either no entity is referenced
     * or if the entity was not yet loaded from the database
     */
    public Optional<E> getValueIfPresent() {
        return Optional.ofNullable(value);
    }

    /**
     * Performs the lookup of the entity with the given id.
     *
     * @param type the type to search for
     * @param id   the id to lookup
     * @return the matching entity wrapped as optional or an empty optional of the entity doesn't exist
     */
    protected abstract Optional<E> find(Class<E> type, I id);

    /**
     * Performs the lookup of the entity with the given id using a secondary node of a DB cluster.
     *
     * @param type the type to search for
     * @param id   the id to lookup
     * @return the matching entity wrapped as optional or an empty optional of the entity doesn't exist
     */
    protected abstract Optional<E> findInSecondary(Class<E> type, I id);

    /**
     * Sets the entity being referenced.
     *
     * @param value the entity being referenced or <tt>null</tt> if no entity is referenced.
     */
    public void setValue(@Nullable E value) {
        this.value = value;
        if (value == null || value.isNew()) {
            this.id = null;
        } else {
            this.id = value.getId();
        }
    }

    /**
     * Determines if the entity object for the referenced id is loaded.
     *
     * @return <tt>true</tt> if the object is present, <tt>false</tt> otherwise
     */
    public boolean isValueLoaded() {
        return id == null || value != null;
    }

    /**
     * Determines if an entity is referenced.
     * <p>
     * The referenced entity might still not be loaded yet.
     *
     * @return <tt>true</tt> if an entity is referenced (the stored id is not <tt>null</tt>). <tt>false</tt> otherwise
     */
    public boolean isFilled() {
        return id != null;
    }

    /**
     * Opposite of {@link #isFilled()}.
     *
     * @return <tt>true</tt> if no entity is referenced, <tt>false</tt> if a non null id is present.
     */
    public boolean isEmpty() {
        return id == null;
    }

    /**
     * Determines if the referenced entity is equal to the given entity or id.
     *
     * @param entityOrId the entity or id to check for
     * @return <tt>true</tt> if the entities are the same (based on their id), <tt>false</tt> otherwise.
     */
    public boolean is(Object entityOrId) {
        if (Strings.isEmpty(entityOrId)) {
            return isEmpty();
        }

        if (entityOrId instanceof BaseEntity<?>) {
            return Objects.equals(((BaseEntity<?>) entityOrId).getId(), id);
        }

        return Objects.equals(coerceToId(entityOrId), id);
    }

    /**
     * Converts the given object into an id.
     *
     * @param id the value to coerce
     * @return the value represented in the correct type
     */
    protected abstract I coerceToId(@Nonnull Object id);

    /**
     * Determines if the given entity value was not yet persisted to the database.
     * <p>
     * For obvious reasons, a non persisted entity cannot be referenced once the referencing entity is saved.
     *
     * @return <tt>true</tt> if the referenced entity was not yet saved to the database, <tt>false</tt> otherwise
     */
    public boolean containsNonpersistentValue() {
        return value != null && (id == null || value.isNew());
    }

    @Override
    public String toString() {
        if (value != null) {
            return value.toString();
        } else if (isFilled()) {
            return String.valueOf(id);
        } else {
            return "(empty)";
        }
    }
}
