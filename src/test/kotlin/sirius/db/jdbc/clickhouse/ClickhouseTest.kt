/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.clickhouse

import sirius.db.jdbc.OMA
import sirius.db.jdbc.batch.BatchContext
import sirius.db.jdbc.batch.InsertQuery
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoField

class ClickhouseSpec extends BaseSpecification {

    @Part
    static OMA oma

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "write a test entity and read it back"() {
        given:
        ClickhouseTestEntity e = new ClickhouseTestEntity()
        Instant now = Instant.now().with(ChronoField.MILLI_OF_SECOND, 0)
        e.setDateTime(now)
        e.setDate(LocalDate.now())
        e.setInt8(100)
        e.setInt16(30_000)
        e.setInt32(1_000_000_000)
        e.setInt64(10_000_000_000)
        e.setString("This is a long string")
        e.setFixedString("X")
        e.setaBooleanSetToTrue(true)
        e.setaBooleanSetToFalse(false)
        e.getStringList().add("string1").add("string2")
        e.setEnumValue(ClickhouseTestEntity.TestEnum.Test2)
        when:
        oma.update(e)
        then:
        ClickhouseTestEntity readBack = oma.select(ClickhouseTestEntity.class).
        eq(ClickhouseTestEntity.FIXED_STRING, "X").
        queryFirst()
                and:
                readBack.getInt8() == 100
                readBack.isaBooleanSetToTrue() == true
                readBack.isaBooleanSetToFalse() == false
                readBack.getInt16() == 30_000
                readBack.getInt32() == 1_000_000_000
                readBack.getInt64() == 10_000_000_000
                readBack.getString() == "This is a long string"
                readBack.getFixedString() == "X"
                readBack.getDate() == LocalDate.now()
                readBack.getDateTime() == now
                readBack.getStringList().data() == ["string1", "string2"]
                readBack.getEnumValue() == ClickhouseTestEntity.TestEnum.Test2
    }

    def "batch insert into clickhouse works"() {
        setup:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        InsertQuery<ClickhouseTestEntity> insert = ctx.insertQuery(
                ClickhouseTestEntity.class, false)
        and:
        for (int i = 0; i < 100; i++) {
        ClickhouseTestEntity e = new ClickhouseTestEntity()
        e.setDateTime(Instant.now())
        e.setDate(LocalDate.now())
        e.setInt8(i)
        e.setInt16(i)
        e.setInt32(i)
        e.setInt64(i)
        e.setString("Test")
        e.setFixedString("B")
        e.setInt8WithDefault(0)
        e.setEnumValue(ClickhouseTestEntity.TestEnum.Test1)
        insert.insert(e, true, true)
    }
        and:
        insert.commit()
        then:
        oma.select(ClickhouseTestEntity.class).eq(ClickhouseTestEntity.FIXED_STRING, "B").count() == 100
                cleanup:
                OMA.LOG.INFO(ctx)
                ctx.close()
    }

    def "property with default-value is set to default when null in object"() {
        given:
        ClickhouseTestEntity e = new ClickhouseTestEntity()
        e.setDateTime(Instant.now())
        e.setDate(LocalDate.now())
        e.setInt8(100)
        e.setInt16(30_000)
        e.setInt32(1_000_000_000)
        e.setInt64(10_000_000_000)
        e.setString("This is a long string")
        e.setFixedString("Y")
        e.setInt8WithDefault(null)
        when:
        oma.update(e)
        then:
        ClickhouseTestEntity readBack = oma.select(ClickhouseTestEntity.class).
        eq(ClickhouseTestEntity.FIXED_STRING, "Y").
        queryFirst()
                and:
                readBack.getInt8WithDefault() == 42
    }

    def "property with default-value is set to actual value when set"() {
        given:
        ClickhouseTestEntity e = new ClickhouseTestEntity()
        e.setDateTime(Instant.now())
        e.setDate(LocalDate.now())
        e.setInt8(100)
        e.setInt16(30_000)
        e.setInt32(1_000_000_000)
        e.setInt64(10_000_000_000)
        e.setString("This is a long string")
        e.setFixedString("Z")
        e.setInt8WithDefault(17)
        when:
        oma.update(e)
        then:
        ClickhouseTestEntity readBack = oma.select(ClickhouseTestEntity.class).
        eq(ClickhouseTestEntity.FIXED_STRING, "Z").
        queryFirst()
                and:
                readBack.getInt8WithDefault() == 17
    }

    def "nullable property can be null"() {
        given:
        ClickhouseTestEntity e = new ClickhouseTestEntity()
        e.setDateTime(Instant.now())
        e.setDate(LocalDate.now())
        e.setInt8(100)
        e.setInt16(30_000)
        e.setInt32(1_000_000_000)
        e.setInt64(10_000_000_000)
        e.setString("This is a long string")
        e.setFixedString("A")
        e.setInt8WithDefault(17)
        e.setNullable(null)
        when:
        oma.update(e)
        then:
        ClickhouseTestEntity readBack = oma.select(ClickhouseTestEntity.class).
        eq(ClickhouseTestEntity.FIXED_STRING, "A").
        queryFirst()
                and:
                readBack.getNullable() == null
    }
}
