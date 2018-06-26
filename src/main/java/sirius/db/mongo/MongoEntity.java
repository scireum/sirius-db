/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Index;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

/**
 * Represents the base class for all entities which are managed via {@link Mango} and stored in MongoDB.
 */
@Index(name = "id", columns = "id", columnSettings = Mango.INDEX_ASCENDING)
public abstract class MongoEntity extends BaseEntity<String> {

    /**
     * Contains the id of the entity.
     * <p>
     * This is declared as null allowed, as the id is generated after the before save checks have been executed.
     */
    public static final Mapping ID = Mapping.named("id");
    @NullAllowed
    protected String id;

    @Part
    protected static Mongo mongo;

    @Part
    protected static Mango mango;

    @Override
    protected void assertUnique(Mapping field, Object value, Mapping... within) {
        Finder finder = mongo.find();
        if (!isNew()) {
            finder.where(QueryBuilder.FILTERS.ne(MongoEntity.ID, getId()));
        }
        for (Mapping withinField : within) {
            finder.where(withinField, getDescriptor().getProperty(withinField).getValue(this));
        }
        if (finder.countIn(getDescriptor().getRelationName()) > 0) {
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
        return (BaseMapper<E, C, Q>) mango;
    }

    @Override
    public String getId() {
        return id;
    }

    protected void setId(String id) {
        this.id = id;
    }
}
