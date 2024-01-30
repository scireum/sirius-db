/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mixing.Mixing
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class LocalDateTimePropertyTest {
    @Test
    fun `local date works when transformed`() {
        val dateTimeProperty = mixing.getDescriptor(DateEntity::class.java).getProperty(DateEntity.LOCAL_DATE_TIME)
        val localDateTime = LocalDateTime.now()
        val localDate = localDateTime.toLocalDate()

        assertEquals(
                localDateTime.truncatedTo(ChronoUnit.MILLIS),
                dateTimeProperty.transformValue(Value.of(localDateTime))
        )
        assertEquals(
                LocalDateTime.of(localDate, LocalTime.MIDNIGHT),
                dateTimeProperty.transformValue(Value.of(localDate))
        )
    }

    companion object {
        @Part
        private lateinit var mixing: Mixing
    }
}
