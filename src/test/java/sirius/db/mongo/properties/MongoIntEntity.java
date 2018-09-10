/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mongo.MongoEntity;

public class MongoIntEntity extends MongoEntity {

    public static final Mapping TEST_INT_PRIMITIVE = Mapping.named("testIntPrimitive");
    private int testIntPrimitive;

    public static final Mapping TEST_OBJECT_PRIMITIVE = Mapping.named("testIntObject");
    private Integer testIntObject;

    public int getTestIntPrimitive() {
        return testIntPrimitive;
    }

    public void setTestIntPrimitive(int testIntPrimitive) {
        this.testIntPrimitive = testIntPrimitive;
    }

    public Integer getTestIntObject() {
        return testIntObject;
    }

    public void setTestIntObject(Integer testIntObject) {
        this.testIntObject = testIntObject;
    }
}
