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

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Represents the base class for all entities which can be managed using {@link Mixing}.
 * <p>
 * Each field will become a property, unless it is annotated with {@link Transient}.
 * <p>
 * The framework highly encourages composition over inheritance. Therefore {@link Composite} fields will directly
 * result in the equivalent properties required to store the fields declared there. Still inheritance might be
 * useful and is fully supported for both, entities and composites.
 * <p>
 * What is not supported, is merging distinct subclasses into one table or other weired inheritance methods. Therefore
 * all superclasses should be abstract.
 * <p>
 * Additionally all <tt>Mixins</tt> {@link sirius.db.mixing.annotations.Mixin} will be used to add properties to the
 * entity. This is especially useful to extend existing entities from within customizations.
 *
 * @param <I> the type of the ID used by subclasses
 */
public abstract class BaseEntity<I> extends Mixable {

    @Part
    protected static Mixing mixing;

    @Transient
    protected int version = 0;

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

    /**
     * Returns the id of the entity.
     *
     * @return the id of the entity
     */
    @Nullable
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
        return mixing.getNameForType(getClass());
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
        return mixing.getUniqueName(getTypeName(), getId());
    }

    /**
     * Provides the {@link BaseMapper mapper} which is used to actually manage the entity.
     *
     * @param <E> the entity type of the mapper
     * @param <Q> the query type of the mapper
     * @return the mapper which is in charge of this entity.
     */
    public abstract <E extends BaseEntity<?>, Q extends Query<Q, E>> BaseMapper<E, Q> getMapper();

    /**
     * Ensures that the given value in the given field is unique within the given side constraints.
     *
     * @param field  the field to check
     * @param value  the value to be unique
     * @param within the side constraints within the value must be unique
     * @throws sirius.kernel.health.HandledException if the value isn't unique
     */
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
     * Returns the version of the entity.
     *
     * @return the version of the entity
     */
    public int getVersion() {
        return version;
    }

    /**
     * Note that only the framework must use this to specify the version of the entity.
     *
     * @param version the version of this entity
     */
    public void setVersion(int version) {
        this.version = version;
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
