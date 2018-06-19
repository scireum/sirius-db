/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.es.ElasticEntity;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.db.mongo.types.MongoRefList;

public class RefListElasticEntity extends ElasticEntity {

    private final MongoRefList<RefListMongoEntity> ref =
            new MongoRefList<>(RefListMongoEntity.class, BaseEntityRef.OnDelete.CASCADE);

    public MongoRefList<RefListMongoEntity> getRef() {
        return ref;
    }
}
