/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.db.es.Elastic
import sirius.db.jdbc.OMA
import sirius.db.mongo.Mango
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

import java.time.Duration

class BaseEntityRefSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            @Part
            private static OMA oma

            @Part
            private static Mango mango

            def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "cascade from JDBC to ES and Mongo works"() {
        when:
        RefEntity refEntity = new RefEntity()
        oma.update(refEntity)

        RefElasticEntity refElasticEntity = new RefElasticEntity()
        refElasticEntity.getRef().setValue(refEntity)
        elastic.update(refElasticEntity)
        elastic.refresh(RefElasticEntity.class)

                RefMongoEntity refMongoEntity = new RefMongoEntity()
                refMongoEntity.getRef().setValue(refEntity)
                mango.update(refMongoEntity)
                and:
                oma.delete(refEntity)
                elastic.refresh(RefElasticEntity.class)
                then:
                !elastic.find(RefElasticEntity.class, refElasticEntity.getId()).isPresent()
                        and:
                !mango.find(RefMongoEntity.class, refMongoEntity.getId()).isPresent()
    }

    def "cascade from ES to JDBC works"() {
        when:
        RefElasticEntity refElasticEntity = new RefElasticEntity()
        elastic.update(refElasticEntity)
        RefEntity refEntity = new RefEntity()
        refEntity.getElastic().setValue(refElasticEntity)
        oma.update(refEntity)
        and:
        elastic.delete(refElasticEntity)
        then:
        !oma.find(RefEntity.class, refEntity.getId()).isPresent()
    }

    def "cascade from Mongo to JDBC works"() {
        when:
        RefMongoEntity refMongoEntity = new RefMongoEntity()
        mango.update(refMongoEntity)
        RefEntity refEntity = new RefEntity()
        refEntity.getMongo().setValue(refMongoEntity)
        oma.update(refEntity)
        and:
        mango.delete(refMongoEntity)
        then:
        !oma.find(RefEntity.class, refEntity.getId()).isPresent()
    }

    def "writeOnce semantics are enforced"() {
        when:
        WriteOnceParentEntity parent = new WriteOnceParentEntity()
        oma.update(parent)
        and:
        WriteOnceChildEntity child = new WriteOnceChildEntity()
        child.getParent().setValue(parent)
        oma.update(child)
        and:
        WriteOnceParentEntity anotherParent = new WriteOnceParentEntity()
        oma.update(anotherParent)
        and:
        child.getParent().setValue(anotherParent)
        oma.update(child)
        then:
        thrown(HandledException)
    }

    def "writeOnce semantics permit a non-changing update"() {
        when:
        WriteOnceParentEntity parent = new WriteOnceParentEntity()
        oma.update(parent)
        and:
        WriteOnceChildEntity child = new WriteOnceChildEntity()
        child.getParent().setValue(parent)
        oma.update(child)
        and:
        child.getParent().setValue(parent)
        oma.update(child)
        then:
        notThrown(HandledException)
    }

}
