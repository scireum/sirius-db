/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.es.types.ElasticRefList;
import sirius.db.mixing.annotations.ComplexDelete;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.db.mongo.MongoEntity;

@ComplexDelete(false)
public class RefListMongoEntity extends MongoEntity {

    @NullAllowed
    private final ElasticRefList<RefListElasticEntity> ref =
            new ElasticRefList<>(RefListElasticEntity.class, BaseEntityRef.OnDelete.SET_NULL);

    public ElasticRefList<RefListElasticEntity> getRef() {
        return ref;
    }
}
