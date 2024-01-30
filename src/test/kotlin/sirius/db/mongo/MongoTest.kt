/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.KeyGenerator
import sirius.db.mixing.Mapping
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class MongoTest {
    companion object {
        @Part
        private lateinit var mongo: Mongo

        @Part
        private lateinit var keyGen: KeyGenerator
    }

    @Test
    fun `basic read and write works`() {
        val testString = System.currentTimeMillis().toString()
        val result = mongo.insert().set("test", testString).set("id", keyGen.generateId()).into("test")

        assertEquals(
                testString,
                mongo.find().where("id", result.getString("id")).singleIn("test").map { d -> d.getString("test") }
                        .orElse(null)
        )
    }

    @Test
    fun `read from secondary works`() {
        val testString = System.currentTimeMillis().toString()
        val result = mongo.insert().set("test", testString).set("id", keyGen.generateId()).into("test")

        assertEquals(
                testString,
                mongo.findInSecondary().where("id", result.getString("id")).singleIn("test")
                        .map { d -> d.getString("test") }
                        .orElse(null)
        )
    }

    @Test
    fun `sort works for singleIn`() {
        mongo.insert().set("sortBy", 1).set("id", keyGen.generateId()).into("test")
        val result2 = mongo.insert().set("sortBy", 3).set("id", keyGen.generateId()).into("test")
        mongo.insert().set("sortBy", 2).set("id", keyGen.generateId()).into("test")


        assertEquals(result2.getString("id"), mongo.find()
                .orderByDesc("sortBy")
                .singleIn("test")
                .map { entity -> entity.getString("id") }
                .orElse(null))
    }

    @Test
    fun `aggregation works`() {
        mongo.insert().set("filter", 1).set("value", 9).set("id", keyGen.generateId()).into("test2")
        mongo.insert().set("filter", 4).set("value", 29).set("id", keyGen.generateId()).into("test2")
        mongo.insert().set("filter", 2).set("value", 22).set("id", keyGen.generateId()).into("test2")

        assertTrue {
            mongo.find()
                    .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 5))
                    .aggregateIn("test2", Mapping.named("value"), "\$sum").isNull
        }

        assertEquals(
                60, mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$sum").asInt(0)
        )

        assertEquals(
                51, mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$sum").asInt(0)
        )

        assertEquals(
                20.0, mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$avg").asDouble(0.0)
        )

        assertEquals(
                25.5, mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$avg").asDouble(0.0)
        )

        assertEquals(
                9, mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$min").asInt(0)
        )

        assertEquals(
                22, mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$min").asInt(0)
        )

        assertEquals(
                29, mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$max").asInt(0)
        )

        assertEquals(
                29, mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$max").asInt(0)
        )

        assertEquals(
                listOf(9, 29, 22), mongo.find()
                .aggregateIn("test2", Mapping.named("value"), "\$push").get(List::class.java)
        )

        assertEquals(
                listOf(29, 22), mongo.find()
                .where(QueryBuilder.FILTERS.gte(Mapping.named("filter"), 2))
                .aggregateIn("test2", Mapping.named("value"), "\$push").get(List::class.java)
        )
    }
}
