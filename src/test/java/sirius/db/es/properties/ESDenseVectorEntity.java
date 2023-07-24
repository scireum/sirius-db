/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import sirius.db.es.ElasticEntity;
import sirius.db.es.types.DenseVector;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;

public class ESDenseVectorEntity extends ElasticEntity {

    public static final Mapping TEST_STRING = Mapping.named("testString");
    @NullAllowed
    private String testString;

    public static final Mapping DENSE_VECTOR = Mapping.named("denseVector");
    private final DenseVector denseVector = new DenseVector(3, DenseVector.Similarity.COSINE, true);

    public DenseVector getDenseVector() {
        return denseVector;
    }

    public String getTestString() {
        return testString;
    }

    public void setTestString(String testString) {
        this.testString = testString;
    }
}
