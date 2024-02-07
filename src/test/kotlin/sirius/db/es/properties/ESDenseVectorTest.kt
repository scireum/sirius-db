/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import sirius.db.es.Elastic
import sirius.db.es.NearestNeighborsSearch
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.Duration

class ESDenseVectorSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "reading and writing works"() {
        given:
        ESDenseVectorEntity test = new ESDenseVectorEntity()

        when:
        float[] vector = new float[3]
        vector[0] = 1f
        vector[1] = 2f
        vector[2] = 3f
        test.getDenseVector().storeVector(vector)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getDenseVector().loadVector()[0] == 1f
        and:
        test.getDenseVector().loadVector()[1] == 2f
        and:
        test.getDenseVector().loadVector()[2] == 3f
    }

    def "knn search works"() {
        given:
        ESDenseVectorEntity test = new ESDenseVectorEntity()

        when:
        float[] vector = new float[3]
        vector[0] = 1f
        vector[1] = 2f
        vector[2] = 3f
        test.getDenseVector().storeVector(vector)
        test.setTestString("FOO")
        and:
        elastic.update(test)
        and:
        elastic.refresh(ESDenseVectorEntity.class)
                then:
                elastic.select(ESDenseVectorEntity.class)
                .knn(new NearestNeighborsSearch(ESDenseVectorEntity.DENSE_VECTOR, vector, 1, 10).filter(Elastic.FILTERS.eq(
                        ESDenseVectorEntity.TEST_STRING,
                        "FOO"))).first().isPresent()
                and:
                elastic.select(ESDenseVectorEntity.class)
                .knn(new NearestNeighborsSearch(ESDenseVectorEntity.DENSE_VECTOR, vector, 1, 10).filter(Elastic.FILTERS.eq(
                        ESDenseVectorEntity.TEST_STRING,
                        "BAR"))).first().isEmpty()
    }

}
