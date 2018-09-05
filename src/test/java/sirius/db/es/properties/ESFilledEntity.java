/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import sirius.db.es.ElasticEntity;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;

public class ESFilledEntity extends ElasticEntity {

    public static final Mapping TEST_FIELD = Mapping.named("testField");
    @NullAllowed
    private String testField;

    public String getTestField() {
        return testField;
    }

    public void setTestField(String testField) {
        this.testField = testField;
    }
}
