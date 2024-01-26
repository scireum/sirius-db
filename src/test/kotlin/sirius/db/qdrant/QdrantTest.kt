/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.util.Tensors
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.util.stream.Collectors
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class QdrantTest {

    companion object {
        @Part
        private lateinit var qdrant: Qdrant
    }

    @Test
    fun `create collection works`() {
        qdrant.db().deleteCollection("test")
        qdrant.db().createCollection("test", 10, QdrantDatabase.Similarity.COSINE)

        assertTrue { qdrant.db().collectionExists("test") }
    }

    @Test
    fun `insert works`() {
        qdrant.db().deleteCollection("test")
        qdrant.db().ensureCollectionExists("test", 10, QdrantDatabase.Similarity.COSINE)
        qdrant.db().upsert(
                "test", listOf(
                Point(
                        Point.deriveId("1"), Tensors.fromList(listOf(1, 0, 0, 0, 0, 0, 0, 0, 0, 0))
                ),
                Point(Point.deriveId("2"), Tensors.fromList(listOf(2, 0, 0, 0, 0, 0, 0, 0, 0, 0))),
                Point(Point.deriveId("3"), Tensors.fromList(listOf(3, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
        )
        )

        assertEquals(3, qdrant.db().countPoints("test", true))
    }

    @Test
    fun `query works`() {
        qdrant.db().deleteCollection("test-search")
        qdrant.db().ensureCollectionExists("test-search", 10, QdrantDatabase.Similarity.COSINE)
        qdrant.db().upsert(
                "test-search", listOf(
                Point(
                        Point.deriveId("1"), Tensors.fromList(listOf(1, 0, 0, 0, 0, 0, 0, 0, 0, 0))
                ).withPayload("x", 1),
                Point(Point.deriveId("2"), Tensors.fromList(listOf(2, 0, 2, 0, 0, 0, 0, 0, 0, 0))).withPayload("x", 2),
                Point(Point.deriveId("3"), Tensors.fromList(listOf(3, 0, 3, 0, 7, 0, 0, 0, 0, 0))).withPayload("x", 3)
        )
        )
        val result = qdrant.db().query("test-search", Tensors.fromList(listOf(1, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
                .execute(2, "x").stream()
                .map { it.getPayload(Integer::class.java, "x") to it }
                .collect(Collectors.toList())

        assertEquals(2, result.size)
        assertEquals(1, result[0].first.toInt())
        assertEquals(2, result[1].first.toInt())
    }
}
