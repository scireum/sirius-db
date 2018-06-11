/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.es.ElasticRef;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mongo.MongoRef;

public class RefEntity extends SQLEntity {

    @NullAllowed
    private final ElasticRef<RefElasticEntity> elastic =
            ElasticRef.on(RefElasticEntity.class, BaseEntityRef.OnDelete.CASCADE);

    @NullAllowed
    private final MongoRef<RefMongoEntity> mongo = MongoRef.on(RefMongoEntity.class, BaseEntityRef.OnDelete.CASCADE);

    public ElasticRef<RefElasticEntity> getElastic() {
        return elastic;
    }

    public MongoRef<RefMongoEntity> getMongo() {
        return mongo;
    }
}
