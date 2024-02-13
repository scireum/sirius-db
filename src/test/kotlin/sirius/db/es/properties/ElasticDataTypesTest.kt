/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.Elastic
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Amount
import sirius.kernel.di.std.Part
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class ElasticDataTypesTest {
    @Test
    fun `reading and writing Long works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.longValue = Long.MAX_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Long.MAX_VALUE, esDataTypesEntity.longValue)

        esDataTypesEntity.longValue = 0
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(0L, esDataTypesEntity.longValue)

        esDataTypesEntity.longValue = Long.MIN_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Long.MIN_VALUE, esDataTypesEntity.longValue)
    }

    @Test
    fun `reading and writing Integer works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.intValue = Integer.MAX_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Integer.MAX_VALUE, esDataTypesEntity.intValue)

        esDataTypesEntity.intValue = 0
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(0, esDataTypesEntity.intValue)

        esDataTypesEntity.intValue = Integer.MIN_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Integer.MIN_VALUE, esDataTypesEntity.intValue)
    }

    @Test
    fun `reading and writing long works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.longValue2 = Long.MAX_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Long.MAX_VALUE, esDataTypesEntity.longValue2)

        esDataTypesEntity.longValue2 = 0
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(0, esDataTypesEntity.longValue2)

        esDataTypesEntity.longValue2 = Long.MIN_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Long.MIN_VALUE, esDataTypesEntity.longValue2)
    }

    @Test
    fun `reading and writing int works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.intValue2 = Integer.MAX_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Integer.MAX_VALUE, esDataTypesEntity.intValue2)

        esDataTypesEntity.intValue2 = 0
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(0, esDataTypesEntity.intValue2)

        esDataTypesEntity.intValue2 = Integer.MIN_VALUE
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Integer.MIN_VALUE, esDataTypesEntity.intValue2)
    }

    @Test
    fun `reading and writing String works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.stringValue = "Test"
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals("Test", esDataTypesEntity.stringValue)
    }

    @Test
    fun `reading and writing amount works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.amountValue = Amount.of(400.5)
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(Amount.of(400.5), esDataTypesEntity.amountValue)
    }

    @Test
    fun `reading and writing LocalDate works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.localDateValue = LocalDate.of(2014, 10, 24)
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(LocalDate.of(2014, 10, 24), esDataTypesEntity.localDateValue)
    }

    @Test
    fun `reading and writing LocalDateTime works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.localDateTimeValue = LocalDateTime.of(2014, 10, 24, 14, 30)
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(LocalDateTime.of(2014, 10, 24, 14, 30), esDataTypesEntity.localDateTimeValue)
    }

    @Test
    fun `reading and writing Boolean true works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.boolValue = true
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertTrue { esDataTypesEntity.boolValue }
    }

    @Test
    fun `reading and writing Boolean false works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.boolValue = false
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertFalse { esDataTypesEntity.boolValue }
    }

    @Test
    fun `reading and writing TestEnum works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.testEnum = ESDataTypesEntity.TestEnum.Test1
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(ESDataTypesEntity.TestEnum.Test1, esDataTypesEntity.testEnum)
    }

    @Test
    fun `reading and writing TestEnum as ordinal works`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.testEnum2 = ESDataTypesEntity.TestEnum.Test2
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(ESDataTypesEntity.TestEnum.Test2, esDataTypesEntity.testEnum2)
    }

    @Test
    fun `reading and writing SQLEntityRefs work`() {
        var esDataTypesEntity = ESDataTypesEntity()
        esDataTypesEntity.sqlEntityRef.setId(1)
        elastic.update(esDataTypesEntity)
        esDataTypesEntity = elastic.refreshOrFail(esDataTypesEntity)

        assertEquals(1L, esDataTypesEntity.sqlEntityRef.id)
        assertTrue { esDataTypesEntity.sqlEntityRef.id is Long }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            elastic.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
