/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.di.std.Part;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents a reference from one entity to another.
 * <p>
 * Instead of directly keeping the entity in a Java field, it is wrapped in an <tt>EntityRef</tt>. This leads to clean
 * semantics for lazy loading as both the ID and (if fetched) the value are stored in this wrapper.
 */
public class EntityRef<E extends Entity> {

    public enum OnDelete {
        CASCADE, SET_NULL, REJECT, SOFT_CASCADE, LAZY_CASCADE
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

    public static <E extends Entity> EntityRef<E> on(Class<E> type, OnDelete deleteHandler) {
        return new EntityRef<>(type, deleteHandler);
    }

    public Class<E> getType() {
        return type;
    }

    public OnDelete getDeleteHandler() {
        return deleteHandler;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
        if (value != null && (id == null || !id.equals(value.getId()))) {
            this.value = null;
        }
    }

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

    public void setValue(E value) {
        this.value = value;
        if (value == null) {
            this.id = null;
        } else {
            this.id = value.getId();
        }
    }

    public boolean isValueLoaded() {
        return id == null || value != null;
    }

    public boolean isFilled() {
        return id != null;
    }

    public boolean isEmpty() {
        return id == null;
    }

    public boolean is(E entity) {
        return entity == null ? isEmpty() : id != null && entity.getId() == id;
    }

    public boolean is(Long otherId) {
        return otherId == null ? isEmpty() : id != null && Objects.equals(otherId, id);
    }

    public boolean containsNonpersistentValue() {
        return id == null && value != null;
    }
}
