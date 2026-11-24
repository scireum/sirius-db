/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.redis

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.extension.ExtendWith
import redis.clients.jedis.params.SetParams
import sirius.kernel.SiriusExtension
import sirius.kernel.async.CallContext
import sirius.kernel.di.std.Part
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class RedisTest {

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

    @Test
    fun `a free lock can be acquired and released`() {
        val lock = uniqueLockName()

        assertFalse(redis.isLocked(lock))
        assertTrue(redis.tryLock(lock, null, LOCK_TIMEOUT))
        assertTrue(redis.isLocked(lock))

        redis.unlock(lock)

        assertFalse(redis.isLocked(lock))
    }

    @Test
    fun `a held lock cannot be acquired again`() {
        val lock = uniqueLockName()
        assertTrue(redis.tryLock(lock, null, LOCK_TIMEOUT))

        assertFalse(redis.tryLock(lock, null, LOCK_TIMEOUT))

        redis.unlock(lock)
    }

    @Test
    fun `a lock held by another node is only released when forced`() {
        val lock = uniqueLockName()
        holdLockAsOtherNode(lock)

        redis.unlock(lock)
        assertTrue(redis.isLocked(lock))

        redis.unlock(lock, true)
        assertFalse(redis.isLocked(lock))
    }

    @Test
    fun `tryLocked runs the task while holding the lock and releases it afterwards`() {
        val lock = uniqueLockName()
        var lockedDuringTask = false

        redis.tryLocked(lock, null, LOCK_TIMEOUT) { lockedDuringTask = redis.isLocked(lock) }

        assertTrue(lockedDuringTask)
        assertFalse(redis.isLocked(lock))
    }

    @Test
    fun `tryLocked releases the lock even if the task fails`() {
        val lock = uniqueLockName()

        assertFailsWith<IllegalStateException> {
            redis.tryLocked(lock, null, LOCK_TIMEOUT) { throw IllegalStateException("task failed") }
        }

        assertFalse(redis.isLocked(lock))
    }

    @Test
    fun `tryLocked skips the task if the lock is held elsewhere`() {
        val lock = uniqueLockName()
        holdLockAsOtherNode(lock)
        var taskExecuted = false

        redis.tryLocked(lock, null, LOCK_TIMEOUT) { taskExecuted = true }

        assertFalse(taskExecuted)
        redis.unlock(lock, true)
    }

    @Test
    fun `the lock list reports owner, acquisition time and remaining lifetime`() {
        val lock = uniqueLockName()
        val before = LocalDateTime.now().withNano(0)
        assertTrue(redis.tryLock(lock, null, LOCK_TIMEOUT))

        val info = redis.lockList.firstOrNull { it.name == lock }

        assertNotNull(info)
        assertEquals(CallContext.getNodeName(), info.value)
        assertFalse(info.since.isBefore(before))
        assertTrue(info.ttl in 1..LOCK_TIMEOUT.seconds)
        redis.unlock(lock)
        assertTrue(redis.lockList.none { it.name == lock })
    }

    private fun uniqueLockName() = "redis-test-lock-" + UUID.randomUUID()

    /**
     * Places the given lock in Redis as if it had been acquired by a different node of the cluster.
     *
     * The key layout mirrors the private constants `PREFIX_LOCK` and `SUFFIX_DATE` in [Redis].
     */
    private fun holdLockAsOtherNode(lock: String) {
        redis.exec({ "Simulate a lock held by another node" }) { db ->
            db.set("lock_$lock", "another-node", SetParams.setParams().ex(LOCK_TIMEOUT.seconds))
            db.set("lock_${lock}_date", LocalDateTime.now().toString(), SetParams.setParams().ex(LOCK_TIMEOUT.seconds))
        }
    }

    companion object {
        private val LOCK_TIMEOUT: Duration = Duration.ofSeconds(30)

        @Part
        private lateinit var redis: Redis
    }
}
