/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import sirius.db.es.Elastic
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Amount
import sirius.kernel.di.std.Part

import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime

class ESDataTypesSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def setupSpec() {
        elastic.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "reading and writing Long works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()

        when:
        test.setLongValue(Long.MAX_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLongValue() == Long.MAX_VALUE

        when:
        test.setLongValue(0)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLongValue() == 0

        when:
        test.setLongValue(Long.MIN_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLongValue() == Long.MIN_VALUE
    }

    def "reading and writing Integer works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()

        when:
        test.setIntValue(Integer.MAX_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getIntValue() == Integer.MAX_VALUE

        when:
        test.setIntValue(0)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getIntValue() == 0

        when:
        test.setIntValue(Integer.MIN_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getIntValue() == Integer.MIN_VALUE
    }

    def "reading and writing long works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()

        when:
        test.setLongValue2(Long.MAX_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLongValue2() == Long.MAX_VALUE

        when:
        test.setLongValue2(0)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLongValue2() == 0

        when:
        test.setLongValue2(Long.MIN_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLongValue2() == Long.MIN_VALUE
    }

    def "reading and writing int works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()

        when:
        test.setIntValue2(Integer.MAX_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getIntValue2() == Integer.MAX_VALUE

        when:
        test.setIntValue2(0)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getIntValue2() == 0

        when:
        test.setIntValue2(Integer.MIN_VALUE)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getIntValue2() == Integer.MIN_VALUE
    }

    def "reading and writing String works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setStringValue("Test")
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getStringValue() == "Test"
    }

    def "reading and writing amount works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setAmountValue(Amount.of(400.5))
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getAmountValue() == Amount.of(400.5)
    }

    def "reading and writing LocalDate works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setLocalDateValue(LocalDate.of(2014, 10, 24))
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLocalDateValue() == LocalDate.of(2014, 10, 24)
    }

    def "reading and writing LocalDateTime works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setLocalDateTimeValue(LocalDateTime.of(2014, 10, 24, 14, 30))
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getLocalDateTimeValue() == LocalDateTime.of(2014, 10, 24, 14, 30)
    }

    def "reading and writing Boolean true works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setBoolValue(true)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getBoolValue() == true
    }

    def "reading and writing Boolean false works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setBoolValue(false)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getBoolValue() == false
    }

    def "reading and writing TestEnum works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setTestEnum(ESDataTypesEntity.TestEnum.Test1)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getTestEnum() == ESDataTypesEntity.TestEnum.Test1
    }

    def "reading and writing TestEnum as ordinal works"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.setTestEnum2(ESDataTypesEntity.TestEnum.Test2)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getTestEnum2() == ESDataTypesEntity.TestEnum.Test2
    }

    def "reading and writing SQLEntityRefs work"() {
        given:
        ESDataTypesEntity test = new ESDataTypesEntity()
        when:
        test.getSqlEntityRef().setId(1)
        and:
        elastic.update(test)
        and:
        test = elastic.refreshOrFail(test)
        then:
        test.getSqlEntityRef().getId() == 1L
        and:
        test.getSqlEntityRef().getId().getClass() == Long.class
    }
}
