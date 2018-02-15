/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Amount
import sirius.kernel.di.std.Part
import sirius.kernel.commons.Value
import java.time.Duration

class DataTypesSpec extends BaseSpecification {

    @Part
    private static OMA oma

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "reading and writing long works"() {
        given:
        DataTypesEntity test = new DataTypesEntity()
        when:
        test.setLongValue(Long.MAX_VALUE)
        and:
        oma.update(test)
        and:
        test = oma.refreshOrFail(test)
        then:
        test.getLongValue() == Long.MAX_VALUE
    }

    def "default values work"() {
        given:
        DataTypesEntity test = new DataTypesEntity()
        Property longValue = test.getDescriptor().getProperty("longValue")
        Property intValue = test.getDescriptor().getProperty("intValue")
        Property stringValue = test.getDescriptor().getProperty("stringValue")
        Property amountValue = test.getDescriptor().getProperty("amountValue")
        Property boolValue = test.getDescriptor().getProperty("boolValue")
        Property localTimeValue = test.getDescriptor().getProperty("localTimeValue")
        Property localDateValue = test.getDescriptor().getProperty("localDateValue")
        Property enumValue = test.getDescriptor().getProperty("enumValue")
        when:
        longValue.parseValue(test, Value.of(null))
        intValue.parseValue(test, Value.of(null))
        stringValue.parseValue(test, Value.of(null))
        amountValue.parseValue(test, Value.of(null))
        boolValue.parseValue(test, Value.of(null))
        localTimeValue.parseValue(test, Value.of(null))
        localDateValue.parseValue(test, Value.of(null))
        enumValue.parseValue(test, Value.of(null))
        and:
        oma.update(test)
        and:
        test = oma.refreshOrFail(test)
        then:
        test.getAmountValue() == Amount.of(300)
        test.getLongValue() == 100L
        test.getIntValue() == 200
        test.getStringValue() == "test"
        test.getBoolValue()
        test.getLocalTimeValue().getHour() == 10
        test.getLocalTimeValue().getMinute() == 15
        test.getLocalTimeValue().getSecond() == 30
        test.getLocalDateValue().getYear() == 2018
        test.getLocalDateValue().getMonth().getValue() == 1
        test.getLocalDateValue().getDayOfMonth() == 1
        test.getTestEnum() == DataTypesEntity.TestEnum.Test2
    }
}
