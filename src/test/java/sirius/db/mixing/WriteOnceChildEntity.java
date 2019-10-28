/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.SQLEntityRef;
import sirius.db.mixing.annotations.ComplexDelete;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.BaseEntityRef;

@ComplexDelete(false)
public class WriteOnceChildEntity extends SQLEntity {

    @NullAllowed
    private final SQLEntityRef<WriteOnceParentEntity> parent =
            SQLEntityRef.writeOnceOn(WriteOnceParentEntity.class, BaseEntityRef.OnDelete.CASCADE);

    public SQLEntityRef<WriteOnceParentEntity> getParent() {
        return parent;
    }
}
