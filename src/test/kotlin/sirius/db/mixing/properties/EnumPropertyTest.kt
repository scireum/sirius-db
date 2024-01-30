/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.properties.ESDataTypesEntity
import sirius.db.mixing.Mixing
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class EnumPropertyTest {
    @Test
    fun `resolving via enum constants and translations works`() {
        val property = mixing.getDescriptor(ESDataTypesEntity::class.java).findProperty("enumValue")
        val esDataTypesEntity = ESDataTypesEntity()

        property?.parseValueFromImport(esDataTypesEntity, Value.of("Test1"))
        assertEquals(ESDataTypesEntity.TestEnum.Test1,esDataTypesEntity.testEnum)

        property?.parseValueFromImport(esDataTypesEntity, Value.of("test1"))
        assertEquals(ESDataTypesEntity.TestEnum.Test1,esDataTypesEntity.testEnum)

        assertThrows<HandledException> { property?.parseValueFromImport(esDataTypesEntity, Value.of("test0")) }

        property?.parseValueFromImport(esDataTypesEntity, Value.of("test25"))
        assertEquals(ESDataTypesEntity.TestEnum.Test2,esDataTypesEntity.testEnum)
    }

    companion object {
        @Part
        private lateinit var mixing: Mixing
    }
}
