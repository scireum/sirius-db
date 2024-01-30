/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.mongo.Mango
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class MongoIntPropertyTest {
    @Test
    fun `read and write of int fields works`() {
        var mongoIntEntity = MongoIntEntity()
        mongoIntEntity.testIntObject = 10
        mongoIntEntity.testIntPrimitive = 100
        mango.update(mongoIntEntity)

        mongoIntEntity = mango.refreshOrFail(mongoIntEntity)

        assertEquals(10, mongoIntEntity.testIntObject)
        assertEquals(100, mongoIntEntity.testIntPrimitive)
    }

    @Test
    fun `no errors if all fields are within the annotated ranges`() {
        val mongoIntEntity = MongoIntEntity()
        mongoIntEntity.testIntObject = 10
        mongoIntEntity.testIntPrimitive = 100
        mongoIntEntity.testIntPositive = 14
        mongoIntEntity.testIntPositiveWithZero = 0
        mongoIntEntity.testIntMaxHundred = 90
        mongoIntEntity.testIntMinHundred = 120
        mongoIntEntity.testIntTwentys = 23

        assertDoesNotThrow { mango.update(mongoIntEntity) }
    }

    @Test
    fun `error for non positive field`() {
        val mongoIntEntity = MongoIntEntity()
        mongoIntEntity.testIntObject = 10
        mongoIntEntity.testIntPrimitive = 100
        mongoIntEntity.testIntPositive = -1

        val exception = assertThrows<HandledException> { mango.update(mongoIntEntity) }
        assertEquals(
                (mongoIntEntity.descriptor.getProperty(MongoIntEntity.TEST_INT_POSITIVE)
                        .illegalFieldValue(Value.of(-1))).message, exception.message
        )
    }

    @Test
    fun `error for too small value`() {
        val mongoIntEntity = MongoIntEntity()
        mongoIntEntity.testIntObject = 10
        mongoIntEntity.testIntPrimitive = 100
        mongoIntEntity.testIntMinHundred = 99

        val exception = assertThrows<HandledException> { mango.update(mongoIntEntity) }
        assertEquals(
                (mongoIntEntity.descriptor.getProperty(MongoIntEntity.TEST_INT_MIN_HUNDRED)
                        .illegalFieldValue(Value.of(99))).message, exception.message
        )
    }

    @Test
    fun `error for too big value`() {
        val mongoIntEntity = MongoIntEntity()
        mongoIntEntity.testIntObject = 10
        mongoIntEntity.testIntPrimitive = 100
        mongoIntEntity.testIntMaxHundred = 111

        val exception = assertThrows<HandledException> { mango.update(mongoIntEntity) }
        assertEquals(
                mongoIntEntity.descriptor.getProperty(MongoIntEntity.TEST_INT_MAX_HUNDRED)
                        .illegalFieldValue(Value.of(111)).message, exception.message
        )
    }

    companion object {
        @Part
        private lateinit var mango: Mango
    }
}
