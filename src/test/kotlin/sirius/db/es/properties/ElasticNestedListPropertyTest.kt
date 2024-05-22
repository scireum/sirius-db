/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.Elastic
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals

@ExtendWith(SiriusExtension::class)
class ElasticNestedListPropertyTest {
    @Test
    fun `reading, change tracking and writing works`() {
        val test = ESNestedListEntity()
        test.list.add(ESNestedListEntity.NestedEntity().withValue1("X").withValue2("Y"))
        elastic.update(test)
        elastic.refresh(ESNestedListEntity::class.java)
        var resolved = elastic.refreshOrFail(test)

        assertEquals(1, resolved.list.size())
        assertEquals("X", resolved.list.data()[0].value1)
        assertEquals("Y", resolved.list.data()[0].value2)

        resolved.list.modify()[0].withValue1("Z")
        elastic.update(resolved)
        elastic.refresh(ESNestedListEntity::class.java)
        resolved = elastic.refreshOrFail(test)

        assertEquals(1, resolved.list.size())
        assertEquals("Z", resolved.list.data()[0].value1)
        assertEquals("Y", resolved.list.data()[0].value2)

        resolved.list.modify().removeAt(0)
        elastic.update(resolved)
        elastic.refresh(ESNestedListEntity::class.java)
        resolved = elastic.refreshOrFail(test)

        assertEquals(0, resolved.list.size())
    }

    /**
     * When storing lists of nested objects, ES will ensure that when executing a "nested" query,
     * an entity only matches, if all fields in a single nested objects matches.
     * <p>
     * Otherwise an entity would match if one property in any nested object matches.
     */
    @Test
    fun `searching in nested fields works as expected`() {
        var test = ESNestedListEntity()
        test.list.add(ESNestedListEntity.NestedEntity().withValue1("A").withValue2("B"))
        test.list.add(ESNestedListEntity.NestedEntity().withValue1("C").withValue2("D"))
        elastic.update(test)
        test = ESNestedListEntity()
        test.list.add(ESNestedListEntity.NestedEntity().withValue1("A").withValue2("B"))
        test.list.add(ESNestedListEntity.NestedEntity().withValue1("A").withValue2("D"))
        elastic.update(test)
        elastic.refresh(ESNestedListEntity::class.java)
        val query = elastic.select(ESNestedListEntity::class.java).where(
                Elastic.FILTERS.nested(ESNestedListEntity.LIST)
                        .eq(ESNestedListEntity.LIST.nested(ESNestedListEntity.NestedEntity.VALUE1), "A")
                        .eq(ESNestedListEntity.LIST.nested(ESNestedListEntity.NestedEntity.VALUE2), "D")
                        .build()
        )
        val resolved = query.queryFirst()

        assertEquals(1, query.limit(0).count())
        assertEquals(2, resolved.list.size())
        assertEquals("A", resolved.list.data()[0].value1)
        assertEquals("B", resolved.list.data()[0].value2)
        assertEquals("A", resolved.list.data()[1].value1)
        assertEquals("D", resolved.list.data()[1].value2)
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
