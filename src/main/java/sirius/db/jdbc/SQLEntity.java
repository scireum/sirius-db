/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Transient;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.di.std.Part;

import java.util.Objects;

/**
 * Represents the base class for all entities which are managed via {@link sirius.db.jdbc.OMA} and stored in
 * a JDBC / SQL database.
 */
public abstract class SQLEntity extends BaseEntity<Long> {

    @Part
    protected static OMA oma;

    /**
     * Contains the unique ID of the entity.
     * <p>
     * This is automatically assigned by the database. Never assign manually a value unless you are totally aware
     * of what you're doing. Also you should not use this ID for external purposes (e.g. as customer number or
     * invoice number) as it cannot be changed.
     * <p>
     * This field is marked as transient as this column is automatically managed by the framework.
     */
    public static final Mapping ID = Mapping.named("id");
    protected long id = -1;

    @Transient
    protected Row fetchRow;

    @Override
    public boolean isNew() {
        return id < 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends BaseEntity<?>, C extends Constraint, Q extends Query<Q, E, C>> BaseMapper<E, C, Q> getMapper() {
        return (BaseMapper<E, C, Q>) oma;
    }

    /**
     * Returns the ID of the entity.
     *
     * @return the ID of the entity or a negative value if the entity was not written to the database yet
     */
    @Override
    public Long getId() {
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
        if (id < 0) {
            // Return a hash code appropriate to the implementation of equals.
            return super.hashCode();
        }
        return (int) (id % Integer.MAX_VALUE);
    }

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
        SQLEntity otherEntity = (SQLEntity) other;
        if (isNew()) {
            return otherEntity.isNew() && super.equals(other);
        }

        return Objects.equals(getId(), otherEntity.getId());
    }

    @Override
    protected boolean isUnique(Mapping field, Object value, Mapping... within) {
        SmartQuery<? extends SQLEntity> qry = oma.select(getClass()).eq(field, value);
        for (Mapping withinField : within) {
            qry.eq(withinField, getDescriptor().getProperty(withinField).getValue(this));
        }
        if (!isNew()) {
            qry.ne(ID, getId());
        }
        return !qry.exists();
    }

    /**
     * If the entity was created as the result of a {@link TransformedQuery}, the original {@link Row} is
     * accessible via this method.
     *
     * @return the original row from which the entity was loaded.
     */
    public Row getFetchRow() {
        return fetchRow;
    }
}
