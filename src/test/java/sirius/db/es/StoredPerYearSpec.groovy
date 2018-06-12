/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import sirius.db.es.filter.FieldEqual
import sirius.db.es.filter.Or
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part

import java.time.Duration
import java.time.LocalDateTime

class StoredPerYearSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "autocreating an index and index determination works"() {
        when:
        YearlyTestEntity test = new YearlyTestEntity()
        test.setTimestamp(LocalDateTime.of(2014, 10, 24, 14, 30))
        elastic.update(test)
        Wait.seconds(2)
        then:
        elastic.getLowLevelClient().indexExists("yearlytestentity-2014")
        and:
        elastic.find(YearlyTestEntity.class, test.getId()).isPresent()

        when:
        elastic.delete(test)
        Wait.seconds(2)
        then:
        !elastic.find(YearlyTestEntity.class, test.getId()).isPresent()
    }

    def "autocreating an index and index determination works for batches"() {
        setup:
        BulkContext btx = elastic.batch()
        LocalDateTime testTimestamp = LocalDateTime.of(2008, 10, 01, 8, 0)
        when:
        YearlyTestEntity test = new YearlyTestEntity()
        test.setTimestamp(testTimestamp)
        btx.tryUpdate(test)
        btx.commit()
        Wait.seconds(2)
        test = elastic.select(YearlyTestEntity.class).
                years(2008).
                eq(YearlyTestEntity.TIMESTAMP, testTimestamp).
                first().
                get()
        then:
        elastic.getLowLevelClient().indexExists("yearlytestentity-2008")
        and:
        test != null

        when:
        test.setTimestamp(LocalDateTime.of(2008, 12, 1, 10, 0))
        btx.tryUpdate(test)
        btx.commit()
        Wait.seconds(2)
        then:
        elastic.find(YearlyTestEntity.class, test.getId()).get().getTimestamp().getMonthValue() == 12

        when:
        btx.forceDelete(test)
        btx.commit()
        Wait.seconds(2)
        then:
        !elastic.find(YearlyTestEntity.class, test.getId()).isPresent()
    }

    def "queries select the appropriate indices"() {
        setup:
        BulkContext btx = elastic.batch()
        LocalDateTime testTimestamp1 = LocalDateTime.of(2011, 10, 01, 8, 0)
        LocalDateTime testTimestamp2 = LocalDateTime.of(2012, 10, 01, 8, 0)
        when:
        YearlyTestEntity test1 = new YearlyTestEntity()
        test1.setTimestamp(testTimestamp1)
        elastic.update(test1)
        YearlyTestEntity test2 = new YearlyTestEntity()
        test2.setTimestamp(testTimestamp2)
        elastic.update(test2)
        Wait.seconds(2)
        then:
        elastic.select(YearlyTestEntity.class).
                yearsFromTo(2010, 2012).
                filter(
                        new Or(
                                new FieldEqual(YearlyTestEntity.TIMESTAMP, testTimestamp1),
                                new FieldEqual(YearlyTestEntity.TIMESTAMP, testTimestamp2))).count() == 2
        and:
        elastic.select(YearlyTestEntity.class).
                years(2011).
                filter(
                        new Or(
                                new FieldEqual(YearlyTestEntity.TIMESTAMP, testTimestamp1),
                                new FieldEqual(YearlyTestEntity.TIMESTAMP, testTimestamp2))).count() == 1
        and:
        elastic.select(YearlyTestEntity.class).
                years(2012).
                filter(
                        new Or(
                                new FieldEqual(YearlyTestEntity.TIMESTAMP, testTimestamp1),
                                new FieldEqual(YearlyTestEntity.TIMESTAMP, testTimestamp2))).count() == 1
    }

}
