/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

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
        ClickhouseTestEntity readBack = oma.select(ClickhouseTestEntity.class).queryFirst();
        and:
        readBack.getInt8() == 100
    }

}
