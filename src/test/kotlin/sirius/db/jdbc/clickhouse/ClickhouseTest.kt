/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.clickhouse

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.jdbc.OMA
import sirius.db.jdbc.batch.BatchContext
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.temporal.ChronoField
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class ClickhouseTest {
    @Test
    fun `write a test entity and read it back`() {
        val clickhouseTestEntity = ClickhouseTestEntity()
        val nowInstant = Instant.now().with(ChronoField.MILLI_OF_SECOND, 0)
        val nowDateTime = LocalDateTime.now().withNano(0)
        clickhouseTestEntity.instant = nowInstant
        clickhouseTestEntity.date = LocalDate.now()
        clickhouseTestEntity.dateTime = nowDateTime
        clickhouseTestEntity.int8 = 100
        clickhouseTestEntity.int16 = 30_000
        clickhouseTestEntity.int32 = 1_000_000_000
        clickhouseTestEntity.int64 = 10_000_000_000
        clickhouseTestEntity.string = "This is a long string"
        clickhouseTestEntity.fixedString = "X"
        clickhouseTestEntity.setaBooleanSetToTrue(true)
        clickhouseTestEntity.setaBooleanSetToFalse(false)
        clickhouseTestEntity.stringList.add("string1").add("string2")
        clickhouseTestEntity.enumValue = ClickhouseTestEntity.TestEnum.Test2
        oma.update(clickhouseTestEntity)
        val readBack =
                oma.select(ClickhouseTestEntity::class.java).eq(ClickhouseTestEntity.FIXED_STRING, "X").queryFirst()

        assertTrue { readBack.isaBooleanSetToTrue() }
        assertFalse { readBack.isaBooleanSetToFalse() }

        assertEquals(100, readBack.int8)
        assertEquals(30_000, readBack.int16)
        assertEquals(1_000_000_000, readBack.int32)
        assertEquals(10_000_000_000, readBack.int64)
        assertEquals("This is a long string", readBack.string)
        assertEquals("X", readBack.fixedString)
        assertEquals(LocalDate.now(), readBack.date)
        assertEquals(nowInstant, readBack.instant)
        assertEquals(nowDateTime, readBack.dateTime)
        assertEquals(listOf("string1", "string2"), readBack.stringList.data())
        assertEquals(listOf(), readBack.emptyList.data())
        assertEquals(ClickhouseTestEntity.TestEnum.Test2, readBack.enumValue)
    }

    @Test
    fun `batch insert into clickhouse works`() {
        val batchContext = BatchContext({ "Test" }, Duration.ofMinutes(2))
        val insert = batchContext.insertQuery(
                ClickhouseTestEntity::class.java, false
        )
        for (i in 0..99) {
            val clickhouseTestEntity = ClickhouseTestEntity()
            clickhouseTestEntity.instant = Instant.now()
            clickhouseTestEntity.dateTime = LocalDateTime.now()
            clickhouseTestEntity.date = LocalDate.now()
            clickhouseTestEntity.int8 = i
            clickhouseTestEntity.int16 = i
            clickhouseTestEntity.int32 = i
            clickhouseTestEntity.int64 = i.toLong()
            clickhouseTestEntity.string = "Test"
            clickhouseTestEntity.fixedString = "B"
            clickhouseTestEntity.int8WithDefault = 0
            clickhouseTestEntity.enumValue = ClickhouseTestEntity.TestEnum.Test1
            insert.insert(clickhouseTestEntity, true, true)
        }
        insert.commit()

        assertEquals(
                100,
                oma.select(ClickhouseTestEntity::class.java).eq(ClickhouseTestEntity.FIXED_STRING, "B").count()
        )

        batchContext.close()
    }

    @Test
    fun `property with default-value is set to default when null in object`() {
        val clickhouseTestEntity = ClickhouseTestEntity()
        clickhouseTestEntity.instant = Instant.now()
        clickhouseTestEntity.dateTime = LocalDateTime.now()
        clickhouseTestEntity.date = LocalDate.now()
        clickhouseTestEntity.int8 = 100
        clickhouseTestEntity.int16 = 30_000
        clickhouseTestEntity.int32 = 1_000_000_000
        clickhouseTestEntity.int64 = 10_000_000_000
        clickhouseTestEntity.string = "This is a long string"
        clickhouseTestEntity.fixedString = "Y"
        clickhouseTestEntity.setInt8WithDefault(null)
        oma.update(clickhouseTestEntity)
        val readBack =
                oma.select(ClickhouseTestEntity::class.java).eq(ClickhouseTestEntity.FIXED_STRING, "Y").queryFirst()

        assertEquals(42, readBack.int8WithDefault)
    }

    @Test
    fun `property with default-value is set to actual value when set`() {
        val clickhouseTestEntity = ClickhouseTestEntity()
        clickhouseTestEntity.instant = Instant.now()
        clickhouseTestEntity.dateTime = LocalDateTime.now()
        clickhouseTestEntity.date = LocalDate.now()
        clickhouseTestEntity.int8 = 100
        clickhouseTestEntity.int16 = 30_000
        clickhouseTestEntity.int32 = 1_000_000_000
        clickhouseTestEntity.int64 = 10_000_000_000
        clickhouseTestEntity.string = "This is a long string"
        clickhouseTestEntity.fixedString = "Z"
        clickhouseTestEntity.int8WithDefault = 17
        oma.update(clickhouseTestEntity)
        val readBack = oma.select(ClickhouseTestEntity::class.java).eq(
                ClickhouseTestEntity.FIXED_STRING,
                "Z"
        ).queryFirst()

        assertEquals(17, readBack.int8WithDefault)
    }

    @Test
    fun `nullable property can be null`() {
        val clickhouseTestEntity = ClickhouseTestEntity()
        clickhouseTestEntity.instant = Instant.now()
        clickhouseTestEntity.dateTime = LocalDateTime.now()
        clickhouseTestEntity.date = LocalDate.now()
        clickhouseTestEntity.int8 = 100
        clickhouseTestEntity.int16 = 30_000
        clickhouseTestEntity.int32 = 1_000_000_000
        clickhouseTestEntity.int64 = 10_000_000_000
        clickhouseTestEntity.string = "This is a long string"
        clickhouseTestEntity.fixedString = "A"
        clickhouseTestEntity.int8WithDefault = 17
        clickhouseTestEntity.nullable = null
        oma.update(clickhouseTestEntity)
        val readBack =
                oma.select(ClickhouseTestEntity::class.java).eq(ClickhouseTestEntity.FIXED_STRING, "A").queryFirst()

        assertNull(readBack.nullable)
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            oma.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
