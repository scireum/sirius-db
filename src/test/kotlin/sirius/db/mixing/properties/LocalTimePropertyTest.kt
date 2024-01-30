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
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class LocalTimePropertyTest {
    @Test
    fun `local time works when transformed`() {
        val timeProperty = mixing.getDescriptor(DateEntity::class.java).getProperty(DateEntity.LOCAL_TIME)
        val localDateTime = LocalDateTime.now()
        val localTime = localDateTime.toLocalTime()

        assertEquals(localTime, timeProperty.transformValue(Value.of(localTime)))
        assertEquals(localTime, timeProperty.transformValue(Value.of(localDateTime)))
    }

    companion object {
        @Part
        private lateinit var mixing: Mixing
    }
}
