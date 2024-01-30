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

class LocalDatePropertySpec extends BaseSpecification {

    @Part
    private static Mixing mixing

            def "local date works when transformed"() {
        when:
        Property dateProperty = mixing.getDescriptor(DateEntity.class).getProperty(DateEntity.LOCAL_DATE)
                LocalDateTime localDateTime = LocalDateTime.now()
                LocalDate localDate = localDateTime.toLocalDate()
                then:
                dateProperty.transformValue(Value.of(localDate)) == localDate
                and:
                dateProperty.transformValue(Value.of(localDateTime)) == localDate
    }

}
