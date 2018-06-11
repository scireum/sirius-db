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

class ClickhouseSpec extends BaseSpecification {

    @Part
    static OMA oma

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "write a test entity and read it back"() {
        given:
        ClickhouseTestEntity e = new ClickhouseTestEntity()
        e.setDateTime(Instant.now())
        e.setDate(LocalDate.now())
        e.setInt8(100)
        e.setInt16(30_000)
        e.setInt32(1_000_000_000)
        e.setInt64(10_000_000_000)
        e.setString("This is a long string")
        e.setFixedString("X")
        when:
        oma.update(e)
        then:
        ClickhouseTestEntity readBack = oma.select(ClickhouseTestEntity.class).
                eq(ClickhouseTestEntity.FIXED_STRING, "X").
                queryFirst()
        and:
        readBack.getInt8() == 100
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

}
