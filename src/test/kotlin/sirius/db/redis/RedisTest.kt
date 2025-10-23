/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class RedisTest {

    companion object {
        @Part
        private lateinit var redis: Redis
    }

    @Test
    fun `getInfo works`() {
        val info = redis.info

        assert(info.isNotEmpty())
        assertNotNull(info["redis_version"])
    }

    @Test
    fun `basic GET SET works`() {
        val testString = System.currentTimeMillis().toString()

        redis.exec({ -> "Setting a test value" }, { db -> db.set("TEST", testString) })

        assertEquals(redis.query({ -> "Getting a test value" }, { db -> db.get("TEST") }), testString)
    }
}
