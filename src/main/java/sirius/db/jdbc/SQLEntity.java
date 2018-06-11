/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.jdbc.constraints.FieldOperator;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.Composite;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Query;
import sirius.db.mixing.annotations.Mixin;
import sirius.db.mixing.annotations.Transient;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

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
    public <E extends BaseEntity<?>, Q extends Query<Q, E>> BaseMapper<E, Q> getMapper() {
        return (BaseMapper<E, Q>) oma;
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
    protected void assertUnique(Mapping field, Object value, Mapping... within) {
        SmartQuery<? extends SQLEntity> qry = oma.select(getClass()).eq(field, value);
        for (Mapping withinField : within) {
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

    public Row getFetchRow() {
        return fetchRow;
    }
}
