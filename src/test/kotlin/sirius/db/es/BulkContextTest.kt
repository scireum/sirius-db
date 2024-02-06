/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

import java.time.Duration

class BulkContextSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

            def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "batch insert works"() {
        setup:
        BulkContext btx = elastic.batch()
        elastic.select(BatchTestEntity.class).delete()
                when:
        btx.tryUpdate(new BatchTestEntity().withValue(1))
        btx.tryUpdate(new BatchTestEntity().withValue(2))
        btx.tryUpdate(new BatchTestEntity().withValue(3))
        btx.commit()
        elastic.refresh(BatchTestEntity.class)
                then:
                elastic.select(BatchTestEntity.class).count() == 3
    }

    def "batch insert with routing works"() {
        setup:
        BulkContext btx = elastic.batch()
        elastic.select(RoutedBatchTestEntity.class).delete()
                when:
        btx.tryUpdate(new RoutedBatchTestEntity().withValue(1).withValue1(5))
        btx.tryUpdate(new RoutedBatchTestEntity().withValue(2).withValue1(5))
        btx.tryUpdate(new RoutedBatchTestEntity().withValue(3).withValue1(5))
        btx.commit()
        elastic.refresh(RoutedBatchTestEntity.class)
                then:
                elastic.select(RoutedBatchTestEntity.class).routing("5").eq(RoutedBatchTestEntity.VALUE1, 5).count() == 3
    }

    def "optimistic locking with batchcontext works"() {
        setup:
        BulkContext btx = elastic.batch()
        elastic.select(BatchTestEntity.class).delete()
                when:
        BatchTestEntity modified = new BatchTestEntity().withValue(100)
                elastic.update(modified)
                elastic.refresh(BatchTestEntity.class)
                BatchTestEntity original = elastic.refreshOrFail(modified)
                elastic.update(modified.withValue(200))
                elastic.refresh(BatchTestEntity.class)
                btx.tryUpdate(original.withValue(150))
                btx.commit()
                elastic.refresh(BatchTestEntity.class)
                then:
                elastic.refreshOrFail(original).getValue() == 200
    }

    def "overwriting with batchcontext works"() {
        setup:
        BulkContext btx = elastic.batch()
        elastic.select(BatchTestEntity.class).delete()
                when:
        BatchTestEntity modified = new BatchTestEntity().withValue(100)
                elastic.update(modified)
                elastic.refresh(BatchTestEntity.class)
                BatchTestEntity original = elastic.refreshOrFail(modified)
                elastic.update(modified.withValue(200))
                elastic.refresh(BatchTestEntity.class)
                btx.overwrite(original.withValue(150))
                btx.commit()
                elastic.refresh(BatchTestEntity.class)
                then:
                elastic.refreshOrFail(original).getValue() == 150
    }

    def "delete with batchcontext works"() {
        setup:
        BulkContext btx = elastic.batch()
        elastic.select(BatchTestEntity.class).delete()
                when:
        BatchTestEntity test = new BatchTestEntity().withValue(100)
                elastic.update(test)
                elastic.refresh(BatchTestEntity.class)
                btx.tryDelete(test)
                btx.commit()
                elastic.refresh(BatchTestEntity.class)
                then:
                !elastic.find(BatchTestEntity.class, test.getId()).isPresent()
    }

    def "beforeSave in bulkContext works"() {
        setup:
        BulkContext btx = elastic.batch()
        ElasticTestEntity test = new ElasticTestEntity()
        test.setFirstname(null)
        when:
        btx.tryUpdate(test)
        then:
        thrown(HandledException)
    }

    def "getFailedIds() works"() {
        setup:
        BulkContext btx = elastic.batch()
        elastic.select(BatchTestEntity.class).delete()
                BatchTestEntity test = new BatchTestEntity().withValue(1)
                BatchTestEntity test2 = new BatchTestEntity().withValue(10)
                when:
        elastic.update(test)
        def refreshed = elastic.refreshOrFail(test)
        elastic.update(test.withValue(2))
        def result = btx.tryUpdate(refreshed.withValue(3)).tryUpdate(test2).commit()
        then:
        !result.isSuccessful()
        and:
        result.getFailedIds().size() == 1
        and:
        result.getFailedIds().contains(refreshed.getId())
    }
}
