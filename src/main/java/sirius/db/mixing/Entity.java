/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Maps;
import sirius.db.jdbc.Row;
import sirius.db.mixing.annotations.Mixin;
import sirius.db.mixing.annotations.Transient;
import sirius.db.mixing.annotations.Versioned;
import sirius.db.mixing.constraints.FieldOperator;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Represents a data object stored into the database.
 * <p>
 * Each non-abstract subclass of <tt>Entity</tt> will become a table in the target database. Each field
 * will become a column, unless it is annotated with {@link Transient}.
 * <p>
 * The framework highly encourages composition over inheritance. Therefore {@link Composite} fields will directly
 * result in the equivalent columns required to store the fields declared there. Still inheritance might be
 * useful and is fully supported for both, entities and composites.
 * <p>
 * What is not supported, is merging distinct subclasses into one table or other weired inheritance methods. Therefore
 * all superclasses should be abstract.
 * <p>
 * Additionally all <tt>Mixins</tt> {@link Mixin} will be used to add columns to the
 * target table. This is especially useful to extend existing entities from within customizations.
 * <p>
 * It also provides a {@link #getIdAsString()} method which returns
 * the database ID as string. However, for new entities (not yet persisted), it returns "new"
 */
public abstract class Entity extends Mixable {

    @Part
    private static Schema schema;

    /**
     * Contains the unique ID of the entity.
     * <p>
     * This is automatically assigned by the database. Never assign manually a value unless you are totally aware
     * of what you're doing. Also you should not use this ID for external purposes (e.g. as customer number or
     * invoice number) as it cannot be changed.
     * <p>
     * This field is marked as transient as this column is automatically managed by the framework.
     */
    public static final Column ID = Column.named("id");
    @Transient
    protected long id = -1;

    /**
     * Contains the current version number read from the database. If optimistic locking is enabled for this
     * entity (via the {@link Versioned} annotation, this is used to protect from
     * conflicting writes an a entity (will throw an {@link OptimisticLockException}.
     * <p>
     * This field is marked as transient as this column is automatically managed by the framework.
     */
    public static final Column VERSION = Column.named("version");
    @Transient
    protected int version = 0;

    @Transient
    protected Map<Property, Object> persistedData = Maps.newHashMap();

    /**
     * If this entity was loaded from a {@link TransformedQuery} (read from a plain JDBC query), this will contain
     * the complete result row. This can be used to access unmapped columns (aggregations or computed ones).
     */
    @Transient
    protected Row fetchRow;

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
        return schema.getDescriptor(getClass());
    }

    /**
     * Determines if the entity is new (not yet written to the database).
     *
     * @return <tt>true</tt> if the entity has not been written to the database yes, <tt>false</tt> otherwise
     */
    public boolean isNew() {
        return id < 0;
    }

    /**
     * Returns the ID of the entity.
     *
     * @return the ID of the entity or a negative value if the entity was not written to the database yet
     */
    public long getId() {
        return id;
    }

    /**
     * Sets the ID of the entity.
     * <p>
     * This method must be used very carefully. For normal operations, this method should ne be used at all
     * as the database ID is managed by the framework.
     *
     * @param id the id of the entity in the database
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Returns a hash code value for the object. This method is
     * supported for the benefit of hash tables such as those provided by
     * {@link java.util.HashMap}.
     * <p>
     * The hash code of an entity is based on its ID. If the entity is not written to the database yet, we use
     * the hash code as computed by {@link Object#hashCode()}. This matches the behaviour of {@link #equals(Object)}.
     *
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        if (id < 0) {
            // Return a hash code appropriate to the implementation of equals.
            return super.hashCode();
        }
        return (int) (id % Integer.MAX_VALUE);
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
        Entity otherEntity = (Entity) other;
        if (isNew()) {
            return otherEntity.isNew() && super.equals(other);
        }

        return id == otherEntity.id;
    }

    /**
     * Returns the complete result row from which this entity was loaded by a <tt>TransformedQuery</tt>
     * <p>
     * If this entity was loaded from a {@link TransformedQuery} (read from a plain JDBC query), this will contain
     * the complete result row. This can be used to access unmapped columns (aggregations or computed ones).
     *
     * @return the complete result row from which this entity was read or <tt>null</tt> if this entity was not
     * read from a <tt>TransformedQuery</tt>
     */
    @Nullable
    public Row getFetchRow() {
        return fetchRow;
    }

    /**
     * Returns an unique name of this entity.
     * <p>
     * This unique string representation of this entity is made up of its type along with its id. It can be resolved
     * using {@link OMA#resolve(String)}.
     *
     * @return an unique representation of this entity or an empty string if the entity was not written to the
     * database yet
     */
    public String getUniqueName() {
        if (isNew()) {
            return "";
        }
        return Schema.getUniqueName(getTypeName(), getId());
    }

    /**
     * Each entity type can be addressed by its class or by a unique name, which is its simple class name in upper
     * case.
     *
     * @return the type name of this entity type
     * @see #getUniqueName()
     * @see OMA#resolve(String)
     * @see Schema#getDescriptor(String)
     */
    public String getTypeName() {
        return Schema.getNameForType(getClass());
    }

    /**
     * Returns the version number of the fetched entity, used for optimistic locking.
     *
     * @return the version number fetched from the database.
     */
    public int getVersion() {
        return version;
    }

    protected void assertUnique(Column field, Object value, Column... within) {
        SmartQuery<? extends Entity> qry = oma.select(getClass()).eq(field, value);
        for (Column withinField : within) {
            qry.eq(withinField, getDescriptor().getProperty(withinField).getValue(this));
        }
        if (!isNew()) {
            qry.where(FieldOperator.on(ID).notEqual(getId()));
        }
        if (qry.exists()) {
            throw Exceptions.createHandled()
                            .withNLSKey("Property.fieldNotUnique")
                            .set("field", getDescriptor().getProperty(field).getLabel())
                            .set("value", NLS.toUserString(value))
                            .handle();
        }
    }

    /**
     * Returns a string representation of the entity ID.
     * <p>
     * If the entity is new, "new" will be returned.
     *
     * @return the entity ID as string or "new" if the entity {@link #isNew()}.
     */
    public String getIdAsString() {
        if (isNew()) {
            return NEW;
        }
        return String.valueOf(getId());
    }

    /**
     * Determines if at least one of the given {@link Column}s were changed in this {@link Entity} since it was last
     * fetched from the database.
     *
     * @param columnsToCheck the columns to check whether they were changed
     * @return <tt>true</tt> if at least one column was changed, <tt>false</tt> otherwise
     */
    public boolean isColumnChanged(Column... columnsToCheck) {
        for (Column column : columnsToCheck) {
            if (getDescriptor().isChanged(this, getDescriptor().getProperty(column))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any {@link Column} of the current {@link Entity} changed.
     *
     * @return <tt>true</tt> if at least one column was changed, <tt>false</tt> otherwise.
     */
    public boolean isAnyColumnChanged() {
        return getDescriptor().getProperties().stream().anyMatch(property -> getDescriptor().isChanged(this, property));
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
