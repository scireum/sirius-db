/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.di.std.Part;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a reference from one entity to another.
 * <p>
 * Instead of directly keeping the entity in a Java field, it is wrapped in an <tt>EntityRef</tt>. This leads to clean
 * semantics for lazy loading as both the ID and (if fetched) the value are stored in this wrapper.
 *
 * @param <E> the generic type of the referenced entity
 */
public class EntityRef<E extends Entity> {

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
         * Same as <tt>CASCADE</tt>, but the constraint is only enfored by the framework not the database.
         */
        SOFT_CASCADE,

        /**
         * Eventually the same as <tt>SOFT_CASCADE</tt>. The referencing entity will be deleted, but not instantly.
         */
        LAZY_CASCADE
    }

    @Part
    private static OMA oma;

    private Class<E> type;
    private OnDelete deleteHandler;
    private Long id;
    private E value;

    private EntityRef(Class<E> type, OnDelete deleteHandler) {
        this.type = type;
        this.deleteHandler = deleteHandler;
    }

    /**
     * Generates an entity reference to the given entity type.
     *
     * @param type          the target type to reference
     * @param deleteHandler determines what happens if the referenced entity is deleted
     * @param <E>           the generic type of the referenced entity
     * @return a new entity ref, representing the given settings
     */
    public static <E extends Entity> EntityRef<E> on(Class<E> type, OnDelete deleteHandler) {
        return new EntityRef<>(type, deleteHandler);
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
     * Returns the id of the referenced entity.
     *
     * @return the id of the referenced entity or <tt>null</tt> if no entity is referenced.
     */
    @Nullable
    public Long getId() {
        return id;
    }

    /**
     * Sets the id of the referenced entity.
     *
     * @param id the id of the referenced entity or <tt>null</tt> if no entity is referenced.
     */
    public void setId(@Nullable Long id) {
        this.id = id;
        if (value != null && (id == null || !id.equals(value.getId()))) {
            this.value = null;
        }
    }

    /**
     * Returns the <tt>Unique Object Name</tt> for the referenced entity.
     *
     * @return the unique object name of the referenced entity or <tt>null</tt> if the reference is empty.
     * @see Entity#getUniqueName()
     * @see OMA#resolve(String)
     * @see Schema#getDescriptor(String)
     */
    public String getUniqueObjectName() {
        if (id == null) {
            return null;
        }

        return Schema.getUniqueName(type, id);
    }

    /**
     * Returns the effective entity object which is referenced.
     * <p>
     * Note, this might cause a database lookup if the entity is not prefetched.
     *
     * @return the entity being referenced or <tt>null</tt> if no entity is referenced.
     */
    @Nullable
    public E getValue() {
        if (value == null && id != null) {
            Optional<E> entity = oma.find(type, id);
            if (entity.isPresent()) {
                value = entity.get();
            } else {
                id = null;
            }
        }
        return value;
    }

    /**
     * Sets the entity being referenced.
     *
     * @param value the entity being referenced or <tt>null</tt> if no entity is referenced.
     */
    public void setValue(@Nullable E value) {
        this.value = value;
        if (value == null) {
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
     * Determines if the referenced entity is equal to the given one.
     *
     * @param entity the entity to check for
     * @return <tt>true</tt> if the entities are the same (based on their id), <tt>false</tt> otherwise.
     */
    public boolean is(E entity) {
        return entity == null ? isEmpty() : id != null && entity.getId() == id;
    }

    /**
     * Determines if the referenced entity has the given id.
     *
     * @param otherId the id to check for
     * @return <tt>true</tt> if the referenced entity has the given id, <tt>false</tt> otherwise.
     */
    public boolean is(Long otherId) {
        return otherId == null ? isEmpty() : id != null && Objects.equals(otherId, id);
    }

    /**
     * Determines if the given entity value was not yet persisted to the database.
     * <p>
     * For obvious reasons, a non persisted entity cannot be referenced once the referencing entity is saved.
     *
     * @return <tt>true</tt> if the referenced entity was not yet saved to the database, <tt>false</tt> otherwise
     */
    public boolean containsNonpersistentValue() {
        return id == null && value != null;
    }
}
