/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.DefaultValue;
import sirius.db.mongo.MongoEntity;

public class MongoDefaultValuesEntity extends MongoEntity {

    public static final Mapping PRIMITIVE_BOOLEAN = Mapping.named("primitiveBoolean");
    @DefaultValue("true")
    private boolean primitiveBoolean = true;

    public static final Mapping PRIMITIVE_INT = Mapping.named("primitiveint");
    private int primitiveInt;

    public boolean isPrimitiveBoolean() {
        return primitiveBoolean;
    }

    public void setPrimitiveBoolean(boolean primitiveBoolean) {
        this.primitiveBoolean = primitiveBoolean;
    }

    public int getPrimitiveInt() {
        return primitiveInt;
    }

    public void setPrimitiveInt(int primitiveInt) {
        this.primitiveInt = primitiveInt;
    }
}
