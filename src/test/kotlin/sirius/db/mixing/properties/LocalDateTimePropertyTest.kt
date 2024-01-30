/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties


import sirius.db.mixing.Mixing
import sirius.db.mixing.Property
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.ChronoUnit

class LocalDateTimePropertySpec extends BaseSpecification {

    @Part
    private static Mixing mixing

            def "local date works when transformed"() {
        when:
        Property dateTimeProperty = mixing.getDescriptor(DateEntity.class).getProperty(DateEntity.LOCAL_DATE_TIME)
                LocalDateTime localDateTime = LocalDateTime.now()
                LocalDate localDate = localDateTime.toLocalDate()
                then:
                dateTimeProperty.transformValue(Value.of(localDateTime)) == localDateTime.truncatedTo(ChronoUnit.MILLIS)
                and:
                dateTimeProperty.transformValue(Value.of(localDate)) == LocalDateTime.of(localDate, LocalTime.MIDNIGHT)
    }

}
