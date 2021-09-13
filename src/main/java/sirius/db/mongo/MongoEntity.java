/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.KeyGenerator;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Index;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Transient;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.di.std.Part;

/**
 * Represents the base class for all entities which are managed via {@link Mango} and stored in MongoDB.
 */
@Index(name = "id", columns = "id", columnSettings = Mango.INDEX_ASCENDING, unique = true)
public abstract class MongoEntity extends BaseEntity<String> {

    /**
     * Contains the {@link #ID} of the entity.
     * <p>
     * This is declared as null allowed, as the id is generated after the before save checks have been executed.
     */
    @NullAllowed
    protected String id;

    @Transient
    protected int version = 0;

    @Part
    protected static Mongo mongo;

    @Part
    protected static Mango mango;

    @Part
    protected static KeyGenerator keyGen;

    @Override
    public boolean isUnique(Mapping field, Object value, Mapping... within) {
        Finder finder = mongo.find().where(field, value);
        if (!isNew()) {
            finder.where(QueryBuilder.FILTERS.ne(MongoEntity.ID, getId()));
        }
        for (Mapping withinField : within) {
            finder.where(withinField, getDescriptor().getProperty(withinField).getValueForDatasource(Mango.class,this));
        }
        return finder.countIn(getDescriptor().getRelationName()) == 0;
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

    /**
     * Generates an id to use in {@link #ID} when creating a new entity.
     *
     * @return the generated id
     */
    protected String generateId() {
        return keyGen.generateId();
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
}
