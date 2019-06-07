/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.SQLEntityRef;
import sirius.db.mixing.annotations.ComplexDelete;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.db.mongo.MongoEntity;

@ComplexDelete(false)
public class RefMongoEntity extends MongoEntity {

    @NullAllowed
    private final SQLEntityRef<RefEntity> ref = SQLEntityRef.on(RefEntity.class, BaseEntityRef.OnDelete.CASCADE);

    public SQLEntityRef<RefEntity> getRef() {
        return ref;
    }
}
