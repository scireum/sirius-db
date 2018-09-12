/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.db.es.Elastic
import sirius.db.mongo.Mango
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part

import java.time.Duration

class BaseEntityRefListSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    @Part
    private static Mango mango

    def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "cascade from Mongo to ES works"() {
        when:
        RefListMongoEntity refMongoEntity = new RefListMongoEntity()
        mango.update(refMongoEntity)
        RefListElasticEntity refElasticEntity = new RefListElasticEntity()
        refElasticEntity.getRef().add(refMongoEntity.getId())
        elastic.update(refElasticEntity)
        Wait.seconds(2)
        and:
        mango.delete(refMongoEntity)
        Wait.seconds(2)
        then:
        !elastic.find(RefListElasticEntity.class, refElasticEntity.getId()).isPresent()
    }

    def "cascade from ES to Mongo works"() {
        when:
        RefListElasticEntity refElasticEntity = new RefListElasticEntity()
        elastic.update(refElasticEntity)
        Wait.seconds(2)
        RefListMongoEntity refMongoEntity = new RefListMongoEntity()
        refMongoEntity.getRef().add(refElasticEntity.getId())
        mango.update(refMongoEntity)
        and:
        elastic.delete(refElasticEntity)
        Wait.seconds(2)
        and:
        def resolved = mango.refreshOrFail(refMongoEntity)
        then:
        !resolved.getRef().contains(refElasticEntity.getId())
    }

}
