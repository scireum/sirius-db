/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.apache.commons.lang3.reflect.FieldUtils
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.jdbc.constraints.CompoundValue
import sirius.db.jdbc.schema.Schema
import sirius.db.mixing.Mixing
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Strings
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException
import java.util.stream.Collectors
import kotlin.test.*

@ExtendWith(SiriusExtension::class)
class SmartQueryKotlinTest {
    @Test
    fun `queryList returns all entities`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.queryList()

        assertEquals(
                listOf("Test", "Hello", "World"),
                result.stream().map { x -> x.value }.collect(Collectors.toList())
        )
    }

    @Test
    fun `streamBlockwise() works`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .fields(SmartQueryTestEntity.VALUE)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)

        assertEquals(
                listOf("Test", "Hello", "World"),
                smartQueryTestEntity.copy().streamBlockwise().map { it.value }.collect(Collectors.toList())
        )
        assertThrows<UnsupportedOperationException> {
            smartQueryTestEntity.copy().skip(1).limit(1).streamBlockwise().count()
        }

        smartQueryTestEntity.copy().distinctFields(SmartQueryTestEntity.TEST_NUMBER).streamBlockwise().count()
    }

    @Test
    fun `count returns the number of entity`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.count()

        assertEquals(3, result)
    }

    @Test
    fun `exists returns a correct value`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.exists()

        assertTrue { result }
    }

    @Test
    fun `exists doesn't screw up the internals of the query`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.exists()

        assertTrue { result }
        assertEquals(3, smartQueryTestEntity.count())
        assertEquals(3, smartQueryTestEntity.queryList().size)
        assertEquals(1, smartQueryTestEntity.queryList()[0].testNumber)
        assertTrue { result }
    }

    @Test
    fun `queryFirst returns the first entity`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.queryFirst()

        assertEquals("Test", result.value)
    }

    @Test
    fun `first returns the first entity`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.first()

        assertEquals("Test", result.get().value)
    }

    @Test
    fun `queryFirst returns null for an empty result`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .eq(SmartQueryTestEntity.VALUE, "xxx")
        val result = smartQueryTestEntity.queryFirst()

        assertNull(result)
    }

    @Test
    fun `first returns an empty optional for an empty result`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .eq(SmartQueryTestEntity.VALUE, "xxx")
        val result = smartQueryTestEntity.first()

        assertNotNull(result)
    }

    @Test
    fun `limit works on a plain list`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .skip(1).limit(2).orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.queryList()

        assertEquals(listOf("Hello", "World"), result.stream().map { x -> x.value }.collect(Collectors.toList()))
    }

    @Test
    fun `limit works on a plain list (skipping native LIMIT)`() {
        val noCapsDB = mockk<Database>()
        every { noCapsDB.hasCapability(any()) } returns false
        every { noCapsDB.connection } returns oma.getDatabase(Mixing.DEFAULT_REALM)!!.connection

        val newOMA = spyk(OMA())
        FieldUtils.writeField(newOMA, "mixing", mixing, true)
        FieldUtils.writeField(newOMA, "schema", mockk<Schema>(), true)
        every { newOMA.getDatabase(Mixing.DEFAULT_REALM) } returns noCapsDB
        val smartQueryTestEntity = newOMA.select(SmartQueryTestEntity::class.java)
                .skip(1).limit(2).orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.queryList()

        assertEquals(listOf("Hello", "World"), result.stream().map { x -> x.value }.collect(Collectors.toList()))
    }

    @Test
    fun `automatic joins work when sorting by a referenced field`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestChildEntity::class.java)
                .orderAsc(
                        SmartQueryTestChildEntity.PARENT.join(
                                SmartQueryTestParentEntity.NAME
                        )
                )
        val result = smartQueryTestEntity.queryList()

        assertEquals(listOf("Child 1", "Child 2"), result.stream().map { x -> x.name }.collect(Collectors.toList()))
    }

    @Test
    fun `automatic joins work when fetching a referenced field`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestChildEntity::class.java)
                .fields(
                        SmartQueryTestChildEntity.PARENT.join(
                                SmartQueryTestParentEntity.NAME
                        )
                )
                .orderAsc(
                        SmartQueryTestChildEntity.PARENT.join(
                                SmartQueryTestParentEntity.NAME
                        )
                )
        val result = smartQueryTestEntity.queryList()

        assertEquals(listOf("Parent 1", "Parent 2"), result.stream().map { x -> x.parent.fetchValue().name }
                .collect(Collectors.toList()))
    }

    @Test
    fun `ids are properly propagated in join fetches `() {
        val smartQueryTestEntity = oma.select(SmartQueryTestChildEntity::class.java)
                .fields(
                        SmartQueryTestChildEntity.PARENT,
                        SmartQueryTestChildEntity.PARENT.join(
                                SmartQueryTestParentEntity.NAME
                        )
                )
        val result = smartQueryTestEntity.queryList()

        assertEquals(listOf(1L, 2L), result.stream().map { x -> x.parent.id }.collect(Collectors.toList()))
    }

    @Test
    fun `automatic joins work when referencing one table in two relations`() {
        val smartQueryTestChildEntity = oma.select(SmartQueryTestChildEntity::class.java)
                .fields(
                        SmartQueryTestChildEntity.PARENT.join(
                                SmartQueryTestParentEntity.NAME
                        ),
                        SmartQueryTestChildEntity.OTHER_PARENT.join(
                                SmartQueryTestParentEntity.NAME
                        )
                )
                .orderAsc(
                        SmartQueryTestChildEntity.PARENT.join(
                                SmartQueryTestParentEntity.NAME
                        )
                )
        val result = smartQueryTestChildEntity.queryList()

        assertEquals(
                listOf("Parent 1Parent 2", "Parent 2Parent 1"), result.stream()
                .map { x ->
                    x.parent.fetchValue().name + x.otherParent.fetchValue().name
                }
                .collect(Collectors.toList())
        )
    }

    @Test
    fun `automatic joins work across several tables`() {
        val smartQueryTestChildChildEntity = oma.select(SmartQueryTestChildChildEntity::class.java).fields(
                SmartQueryTestChildChildEntity.PARENT_CHILD.join(SmartQueryTestChildEntity.PARENT).join(
                        SmartQueryTestParentEntity.NAME
                )
        ).orderAsc(
                SmartQueryTestChildChildEntity.PARENT_CHILD.join(SmartQueryTestChildEntity.PARENT).join(
                        SmartQueryTestParentEntity.NAME
                )
        )
        val result = smartQueryTestChildChildEntity.queryList()

        assertEquals(
                listOf("Parent 1"),
                result.stream().map { x -> x.parentChild.fetchValue().parent.fetchValue().name }
                        .collect(Collectors.toList()))
    }

    @Test
    fun `exists works when referencing a child entity`() {
        val smartQueryTestChildEntity = oma.select(SmartQueryTestParentEntity::class.java).where(
                OMA.FILTERS.existsIn(
                        SmartQueryTestParentEntity.ID,
                        SmartQueryTestChildEntity::class.java,
                        SmartQueryTestChildEntity.PARENT
                )
        )
        val result = smartQueryTestChildEntity.queryList()

        assertEquals(listOf("Parent 1", "Parent 2"), result.stream().map { x -> x.name }.collect(Collectors.toList()))
    }

    @Test
    fun `exists works when referencing a child entity with constraints`() {
        val smartQueryTestChildEntity = oma.select(SmartQueryTestParentEntity::class.java).where(
                OMA.FILTERS.existsIn(
                        SmartQueryTestParentEntity.ID,
                        SmartQueryTestChildEntity::class.java,
                        SmartQueryTestChildEntity.PARENT
                ).where(OMA.FILTERS.eq(SmartQueryTestChildEntity.NAME, "Child 1"))
        )
        val result = smartQueryTestChildEntity.queryList()

        assertEquals(listOf("Parent 1"), result.stream().map { x -> x.name }.collect(Collectors.toList()))
    }

    @Test
    fun `exists works when inverted`() {
        val smartQueryTestChildEntity = oma.select(SmartQueryTestParentEntity::class.java).where(
                OMA.FILTERS.not(
                        OMA.FILTERS.existsIn(
                                SmartQueryTestParentEntity.ID,
                                SmartQueryTestChildEntity::class.java,
                                SmartQueryTestChildEntity.PARENT
                        ).where(OMA.FILTERS.eq(SmartQueryTestChildEntity.NAME, "Child 1"))
                )
        )
        val result = smartQueryTestChildEntity.queryList()

        assertEquals(listOf("Parent 2"), result.stream().map { x -> x.name }.collect(Collectors.toList()))
    }

    @Test
    fun `exists works with complicated columns`() {
        val smartQueryTestChildEntity = oma.select(SmartQueryTestParentEntity::class.java).where(
                OMA.FILTERS.existsIn(
                        CompoundValue(SmartQueryTestParentEntity.ID).addComponent(SmartQueryTestParentEntity.NAME),
                        SmartQueryTestChildEntity::class.java,
                        CompoundValue(SmartQueryTestChildEntity.PARENT).addComponent(
                                SmartQueryTestChildEntity
                                        .PARENT_NAME
                        )
                ).where(OMA.FILTERS.eq(SmartQueryTestChildEntity.NAME, "Child 1"))
        )
        val result = smartQueryTestChildEntity.queryList()

        assertEquals(listOf("Parent 1"), result.stream().map { x -> x.name }.collect(Collectors.toList()))
    }

    @Test
    fun `copy of query does also copy fields`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java)
                .fields(SmartQueryTestEntity.TEST_NUMBER, SmartQueryTestEntity.VALUE)
                .copy()
                .orderAsc(SmartQueryTestEntity.TEST_NUMBER)
        val result = smartQueryTestEntity.queryList()

        assertEquals(listOf("Test", "Hello", "World"), result.stream().map { x -> x.value }
                .collect(Collectors.toList()))
    }

    @Test
    fun `select non existant entity ref`() {
        val testEntityWithNullRef = TestEntityWithNullRef()
        testEntityWithNullRef.name = "bliblablub"
        oma.update(testEntityWithNullRef)
        val result = oma.select(TestEntityWithNullRef::class.java)
                .fields(
                        TestEntityWithNullRef.NAME,
                        TestEntityWithNullRef.PARENT.join(SmartQueryTestParentEntity.NAME),
                        TestEntityWithNullRef.PARENT.join(SmartQueryTestParentEntity.ID)
                )
                .eq(TestEntityWithNullRef.ID, testEntityWithNullRef.getId()).queryList()
        val found = result[0]

        assertTrue { found.parent.isEmpty }
    }

    @Test
    fun `select existing entity ref with automatic id fetching`() {
        val smartQueryTestParentEntity = SmartQueryTestParentEntity()
        smartQueryTestParentEntity.name = "Parent 3"
        oma.update(smartQueryTestParentEntity)

        val testEntityWithNullRef = TestEntityWithNullRef()
        testEntityWithNullRef.name = "bliblablub"
        testEntityWithNullRef.parent.setValue(smartQueryTestParentEntity)
        oma.update(testEntityWithNullRef)

        val result = oma.select(TestEntityWithNullRef::class.java)
                .fields(
                        TestEntityWithNullRef.NAME,
                        SmartQueryTestChildEntity.PARENT.join(SmartQueryTestParentEntity.NAME)
                )
                .eq(TestEntityWithNullRef.ID, testEntityWithNullRef.getId()).queryList()
        val found = result[0]

        assertTrue { found.parent.isFilled }
        assertFalse { found.parent.fetchValue().isNew }
        assertTrue { Strings.isFilled(found.parent.fetchValue().name) }
    }

    @Test
    fun `select a entity with attached mixin by value in mixin`() {
        val smartQueryTestMixinEntity1 = SmartQueryTestMixinEntity()
        smartQueryTestMixinEntity1.value = "testvalue1"
        val mixinEntityMixin = smartQueryTestMixinEntity1.`as`(SmartQueryTestMixinEntityMixin::class.java)
        mixinEntityMixin.mixinValue = "mixinvalue1"
        oma.update(smartQueryTestMixinEntity1)

        val smartQueryTestMixinEntity2 = oma.select(SmartQueryTestMixinEntity::class.java).eq(
                SmartQueryTestMixinEntityMixin.MIXIN_VALUE.inMixin(SmartQueryTestMixinEntityMixin::class.java),
                "mixinvalue1"
        ).queryFirst()

        assertFalse { smartQueryTestMixinEntity1.isNew }
        assertEquals(smartQueryTestMixinEntity1.getId(), smartQueryTestMixinEntity2.getId())
    }

    @Test
    fun `a forcefully failed query does not yield any results`() {
        val smartQueryTestEntity = oma.select(SmartQueryTestEntity::class.java).fail()
        var flag = false
        smartQueryTestEntity.queryList().isEmpty()
        smartQueryTestEntity.iterateAll { _ -> flag = true }

        assertFalse { flag }
        assertEquals(0, smartQueryTestEntity.count())
        assertFalse { smartQueryTestEntity.exists() }
    }

    @Test
    fun `eq with row values works`() {
        var items = oma.select(SmartQueryTestEntity::class.java).where(
                OMA.FILTERS.eq(
                        CompoundValue(SmartQueryTestEntity.VALUE).addComponent(
                                SmartQueryTestEntity
                                        .TEST_NUMBER
                        ),
                        CompoundValue("Test").addComponent(1)
                )
        ).queryList()

        assertEquals(1, items.size)
        assertEquals(1, items[0].testNumber)

        items = oma.select(SmartQueryTestEntity::class.java).where(
                OMA.FILTERS.eq(
                        CompoundValue("Test").addComponent(SmartQueryTestEntity.TEST_NUMBER),
                        CompoundValue(SmartQueryTestEntity.VALUE).addComponent(1)
                )
        ).queryList()

        assertEquals(1, items.size)
        assertEquals(1, items[0].testNumber)
    }

    @Test
    fun `gt with row values works`() {
        val items = oma.select(SmartQueryTestEntity::class.java).where(
                OMA.FILTERS.gt(
                        CompoundValue(SmartQueryTestEntity.VALUE).addComponent(2),
                        CompoundValue("Test").addComponent(SmartQueryTestEntity.TEST_NUMBER)
                )
        ).orderAsc(SmartQueryTestEntity.VALUE).queryList()

        assertEquals(2, items.size)
        assertEquals(1, items[0].testNumber)
        assertEquals(3, items[1].testNumber)
    }


    @Test
    fun `count distinct for one field returns a correct number of entities`() {
        val smartQueryTestCountEntity = oma.select(SmartQueryTestCountEntity::class.java)
                .distinctFields(SmartQueryTestCountEntity.FIELD_ONE)
        val result = smartQueryTestCountEntity.count()

        assertEquals(2, result)
    }

    @Test
    fun `count distinct for two fields returns a correct number of entities`() {
        val smartQueryTestCountEntity = oma.select(SmartQueryTestCountEntity::class.java)
                .distinctFields(
                        SmartQueryTestCountEntity.FIELD_ONE,
                        SmartQueryTestCountEntity.FIELD_TWO
                )
        val result = smartQueryTestCountEntity.count()

        assertEquals(3, result)
    }

    @Test
    fun `count for one field without distinct modifier returns a correct number of entities`() {
        val smartQueryTestCountEntity = oma.select(SmartQueryTestCountEntity::class.java)
                .fields(SmartQueryTestCountEntity.FIELD_ONE)
        val result = smartQueryTestCountEntity.count()

        assertEquals(4, result)
    }

    @Test
    fun `count for two fields without distinct modifier throws an exception`() {
        val smartQueryTestCountEntity = oma.select(SmartQueryTestCountEntity::class.java)
                .fields(
                        SmartQueryTestCountEntity.FIELD_ONE,
                        SmartQueryTestCountEntity.FIELD_TWO
                )

        assertThrows<HandledException> { smartQueryTestCountEntity.count() }
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        @Part
        private lateinit var mixing: Mixing

        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            oma.select(SmartQueryTestEntity::class.java).delete()
            oma.select(SmartQueryTestParentEntity::class.java).delete()
            oma.select(SmartQueryTestChildEntity::class.java).delete()
            oma.select(SmartQueryTestChildChildEntity::class.java).delete()
            oma.select(SmartQueryTestCountEntity::class.java).delete()

            fillSmartQueryTestEntity()
            fillSmartQueryTestChildAndParentEntity()
            fillSmartQueryTestLargeTableEntity()
            fillSmartQueryCountEntity()
        }

        private fun fillSmartQueryTestEntity() {
            var smartQueryTestEntity = SmartQueryTestEntity()
            smartQueryTestEntity.value = "Test"
            smartQueryTestEntity.testNumber = 1
            oma.update(smartQueryTestEntity)
            smartQueryTestEntity = SmartQueryTestEntity()
            smartQueryTestEntity.value = "Hello"
            smartQueryTestEntity.testNumber = 2
            oma.update(smartQueryTestEntity)
            smartQueryTestEntity = SmartQueryTestEntity()
            smartQueryTestEntity.value = "World"
            smartQueryTestEntity.testNumber = 3
            oma.update(smartQueryTestEntity)
        }

        private fun fillSmartQueryTestChildAndParentEntity() {
            val smartQueryTestParentEntity1 = SmartQueryTestParentEntity()
            smartQueryTestParentEntity1.name = "Parent 1"
            oma.update(smartQueryTestParentEntity1)
            val smartQueryTestParentEntity2 = SmartQueryTestParentEntity()
            smartQueryTestParentEntity2.name = "Parent 2"
            oma.update(smartQueryTestParentEntity2)

            var smartQueryTestChildEntity = SmartQueryTestChildEntity()
            smartQueryTestChildEntity.name = "Child 1"
            smartQueryTestChildEntity.parent.setId(smartQueryTestParentEntity1.getId())
            smartQueryTestChildEntity.otherParent.setId(smartQueryTestParentEntity2.getId())
            smartQueryTestChildEntity.parentName = smartQueryTestParentEntity1.name
            oma.update(smartQueryTestChildEntity)

            val smartQueryTestChildChildEntity = SmartQueryTestChildChildEntity()
            smartQueryTestChildChildEntity.name = "ChildChild 1"
            smartQueryTestChildChildEntity.parentChild.setValue(smartQueryTestChildEntity)
            oma.update(smartQueryTestChildChildEntity)

            smartQueryTestChildEntity = SmartQueryTestChildEntity()
            smartQueryTestChildEntity.name = "Child 2"
            smartQueryTestChildEntity.parent.setId(smartQueryTestParentEntity2.getId())
            smartQueryTestChildEntity.otherParent.setId(smartQueryTestParentEntity1.getId())
            smartQueryTestChildEntity.parentName = smartQueryTestParentEntity2.name
            oma.update(smartQueryTestChildEntity)
        }

        private fun fillSmartQueryTestLargeTableEntity() {
            for (i in 0..1099) {
                val smartQueryTestLargeTableEntity = SmartQueryTestLargeTableEntity()
                smartQueryTestLargeTableEntity.testNumber = i
                oma.update(smartQueryTestLargeTableEntity)
            }
        }

        private fun fillSmartQueryCountEntity() {
            val smartQueryTestCountEntity1 = SmartQueryTestCountEntity()
            smartQueryTestCountEntity1.fieldOne = "1"
            smartQueryTestCountEntity1.fieldTwo = "1"
            oma.update(smartQueryTestCountEntity1)

            val smartQueryTestCountEntity2 = SmartQueryTestCountEntity()
            smartQueryTestCountEntity2.fieldOne = "1"
            smartQueryTestCountEntity2.fieldTwo = "2"
            oma.update(smartQueryTestCountEntity2)

            val smartQueryTestCountEntity3 = SmartQueryTestCountEntity()
            smartQueryTestCountEntity3.fieldOne = "3"
            smartQueryTestCountEntity3.fieldTwo = "3"
            oma.update(smartQueryTestCountEntity3)

            val smartQueryTestCountEntity4 = SmartQueryTestCountEntity()
            smartQueryTestCountEntity4.fieldOne = "3"
            smartQueryTestCountEntity4.fieldTwo = "3"
            oma.update(smartQueryTestCountEntity4)
        }
    }
}
