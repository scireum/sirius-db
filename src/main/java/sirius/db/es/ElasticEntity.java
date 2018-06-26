/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

/**
 * Represents the base class for all entities which are managed via {@link Elastic} and stored in Elasticsearch.
 */
public abstract class ElasticEntity extends BaseEntity<String> {

    @Part
    protected static Elastic elastic;

    /**
     * Contains the ID which is auto-generated when inserting a new entity into Elasticsearch.
     * <p>
     * It is {@link NullAllowed} as it is filled during the update but after the save checkes have completed.
     */
    public static final Mapping ID = Mapping.named("id");
    @NullAllowed
    private String id;

    @Override
    protected void assertUnique(Mapping field, Object value, Mapping... within) {
        ElasticQuery<? extends ElasticEntity> qry = elastic.select(getClass()).eq(field, value);
        for (Mapping withinField : within) {
            qry.eq(withinField, getDescriptor().getProperty(withinField).getValue(this));
        }
        if (!isNew()) {
            qry.ne(ID, getId());
        }
        if (qry.exists()) {
            throw Exceptions.createHandled()
                            .withNLSKey("Property.fieldNotUnique")
                            .set("field", getDescriptor().getProperty(field).getLabel())
                            .set("value", NLS.toUserString(value))
                            .handle();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends BaseEntity<?>, C extends Constraint, Q extends Query<Q, E, C>> BaseMapper<E, C, Q> getMapper() {
        return (BaseMapper<E, C, Q>) elastic;
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * Note that only the framework must use this to specify the ID of the entity.
     *
     * @param id the id of this entity
     */
    protected void setId(String id) {
        this.id = id;
    }
}
