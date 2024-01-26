/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis

import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class RedisSpec extends BaseSpecification {

    @Part
    private static Redis redis

            def "basic GET/SET works"() {
        given:
        def testString = String.valueOf(System.currentTimeMillis())
        when:
        redis.exec({ -> "Setting a test value" }, { db -> db.set("TEST", testString) })
        then:
        testString == redis.query({ -> "Getting a test value" }, { db -> db.get("TEST") })

    }

}
