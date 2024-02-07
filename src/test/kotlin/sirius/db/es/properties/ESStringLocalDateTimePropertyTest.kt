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
import sirius.db.mixing.properties.StringMapProperty
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class ESStringLocalDateTimePropertyTest {

    @Test
    fun `reading and writing works for Elasticsearch`() {
        val esStringLocalDateTimeMapEntity = ESStringLocalDateTimeMapEntity()
        val now = LocalDateTime.now()
        esStringLocalDateTimeMapEntity.map.put("a", now)
        elastic.update(esStringLocalDateTimeMapEntity)
        elastic.refresh(ESStringLocalDateTimeMapEntity::class.java)
        var resolved = elastic.refreshOrFail(esStringLocalDateTimeMapEntity)

        assertEquals(1, resolved.map.size())
        assertNotNull(resolved.map.get("a"))
        assertEquals(now.truncatedTo(ChronoUnit.MILLIS), resolved.map.get("a").get())

        resolved.map.modify().remove("a")
        resolved.map.modify()["b"] = null
        elastic.update(resolved)
        elastic.refresh(ESStringLocalDateTimeMapEntity::class.java)
        resolved = elastic.refreshOrFail(resolved)

        assertEquals(1, resolved.map.size())
        assertTrue { resolved.map.containsKey("b") && !resolved.map.get("b").isPresent }
        assertTrue { resolved.map.containsKey("b") }
        assertFalse { resolved.map.get("b").isPresent }
    }

    @Test
    fun `querying date fields works`() {
        val test = ESStringLocalDateTimeMapEntity()
        test.map.put("a", LocalDateTime.now().plusDays(2)).put("b", LocalDateTime.now().plusDays(3))
        elastic.update(test)
        elastic.refresh(ESStringLocalDateTimeMapEntity::class.java)

        assertEquals(
                1,
                elastic.select(ESStringLocalDateTimeMapEntity::class.java).where(
                        Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(
                                Elastic.FILTERS.gt(
                                        ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                                        LocalDateTime.now().plusDays(1)
                                )
                        ).build()
                ).count()
        )
        assertEquals(
                1,
                elastic.select(ESStringLocalDateTimeMapEntity::class.java).where(
                        Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(
                                Elastic.FILTERS.gt(
                                        ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                                        LocalDateTime.now().plusDays(2)
                                )
                        ).build()
                ).count()
        )
        assertEquals(
                0,
                elastic.select(ESStringLocalDateTimeMapEntity::class.java).where(
                        Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(
                                Elastic.FILTERS.and(
                                        Elastic.FILTERS.eq(
                                                ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.KEY), "a"
                                        ),
                                        Elastic.FILTERS.gt(
                                                ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                                                LocalDateTime.now().plusDays(2)
                                        )
                                )
                        ).build()
                ).count()
        )
        assertEquals(
                1,
                elastic.select(ESStringLocalDateTimeMapEntity::class.java).where(
                        Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(
                                Elastic.FILTERS.and(
                                        Elastic.FILTERS.eq(
                                                ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.KEY), "b"
                                        ),
                                        Elastic.FILTERS.gt(
                                                ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                                                LocalDateTime.now().plusDays(2)
                                        )
                                )
                        ).build()
                ).count()
        )
        assertEquals(
                0,
                elastic.select(ESStringLocalDateTimeMapEntity::class.java).where(
                        Elastic.FILTERS.nested(ESStringLocalDateTimeMapEntity.MAP).where(
                                Elastic.FILTERS.gt(
                                        ESStringLocalDateTimeMapEntity.MAP.nested(StringMapProperty.VALUE),
                                        LocalDateTime.now().plusDays(3)
                                )
                        ).build()
                ).count()
        )
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
    }
}
