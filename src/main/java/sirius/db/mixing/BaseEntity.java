/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Maps;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.annotations.Transient;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;

import java.util.Map;

public abstract class BaseEntity<I> extends Mixable {

    @Part
    private static Mixing mixing;

    @Transient
    protected Map<Property, Object> persistedData = Maps.newHashMap();

    /**
     * Contains the constant used to mark a new (unsaved) entity.
     */
    public static final String NEW = "new";

    /**
     * Returns the descriptor which maps the entity to the database table.
     *
     * @return the ddescriptor which is in charge of checking and mapping the entity to the database
     */
    public EntityDescriptor getDescriptor() {
        return mixing.getDescriptor(getClass());
    }

    public abstract I getId();

    /**
     * Determines if the entity is new (not yet written to the database).
     *
     * @return <tt>true</tt> if the entity has not been written to the database yes, <tt>false</tt> otherwise
     */
    public boolean isNew() {
        return getId() == null;
    }

    /**
     * Each entity type can be addressed by its class or by a unique name, which is its simple class name in upper
     * case.
     *
     * @return the type name of this entity type
     * @see #getUniqueName()
     */
    public String getTypeName() {
        return Mixing.getNameForType(getClass());
    }

    /**
     * Returns an unique name of this entity.
     * <p>
     * This unique string representation of this entity is made up of its type along with its id.
     *
     * @return an unique representation of this entity or an empty string if the entity was not written to the
     * database yet
     */
    public final String getUniqueName() {
        if (isNew()) {
            return "";
        }
        return Mixing.getUniqueName(getTypeName(), getId());
    }

    public abstract <E extends BaseEntity<?>, Q extends Query<Q, E>> BaseMapper<E, Q> getMapper();

    //TODO javadoc
    protected abstract void assertUnique(Mapping field, Object value, Mapping... within);

    /**
     * Returns a string representation of the entity ID.
     * <p>
     * If the entity is new, "new" will be returned.
     *
     * @return the entity ID as string or "new" if the entity {@link #isNew()}.
     */
    public final String getIdAsString() {
        if (isNew()) {
            return NEW;
        }
        return String.valueOf(getId());
    }

    /**
     * Determines if at least one of the given {@link Mapping}s were changed in this {@link SQLEntity} since it was last
     * fetched from the database.
     *
     * @param mappingsToCheck the columns to check whether they were changed
     * @return <tt>true</tt> if at least one column was changed, <tt>false</tt> otherwise
     */
    public boolean isChanged(Mapping... mappingsToCheck) {
        for (Mapping mapping : mappingsToCheck) {
            if (getDescriptor().isChanged(this, getDescriptor().getProperty(mapping))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any {@link Mapping} of the current {@link SQLEntity} changed.
     *
     * @return <tt>true</tt> if at least one column was changed, <tt>false</tt> otherwise.
     */
    public boolean isAnyMappingChanged() {
        return getDescriptor().getProperties().stream().anyMatch(property -> getDescriptor().isChanged(this, property));
    }

    /**
     * Returns a hash code value for the object. This method is supported for the benefit of hash tables such as those
     * provided by {@link java.util.HashMap}.
     * <p>
     * The hash code of an entity is based on its ID. If the entity is not written to the database yet, we use
     * the hash code as computed by {@link Object#hashCode()}. This matches the behaviour of {@link #equals(Object)}.
     *
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        if (isNew()) {
            // Return a hash code appropriate to the implementation of equals.
            return super.hashCode();
        }

        return getId().hashCode();
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     * <p>
     * Equality of two entities is based on their type and ID. If an entity is not written to the database yet, we
     * determine equality as computed by {@link Object#equals(Object)}. This matches the behaviour of
     * {@link #hashCode()}.
     *
     * @return {@code true} if this object is the same as the obj
     * argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (!(other.getClass().equals(getClass()))) {
            return false;
        }
        BaseEntity<?> otherEntity = (BaseEntity<?>) other;
        if (isNew()) {
            return otherEntity.isNew() && super.equals(other);
        }

        return Strings.areEqual(getId(), otherEntity.getId());
    }

    @Override
    public String toString() {
        if (isNew()) {
            return "new " + getClass().getSimpleName();
        } else {
            return getUniqueName();
        }
    }
}
