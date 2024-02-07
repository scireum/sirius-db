/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.db.es.Elastic
import sirius.db.es.NearestNeighborsSearch
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class ESDenseVectorTest {
    @Test
    fun `reading and writing works`() {
        var test = ESDenseVectorEntity()
        val vector = arrayOf(1f, 2f, 3f)
        test.denseVector.storeVector(vector)
        elastic.update(test)
        test = elastic.refreshOrFail(test)

        assertEquals(1f, test.denseVector.loadVector()[0])
        assertEquals(2f, test.denseVector.loadVector()[1])
        assertEquals(3f, test.denseVector.loadVector()[2])
    }

    @Test
    fun `knn search works`() {
        val test = ESDenseVectorEntity()
        val vector = floatArrayOf(1f, 2f, 3f)
        test.denseVector.storeVector(vector.toTypedArray())
        test.testString = "FOO"
        elastic.update(test)
        elastic.refresh(ESDenseVectorEntity::class.java)

        assertNotNull(
                elastic.select(ESDenseVectorEntity::class.java).knn(
                        NearestNeighborsSearch(ESDenseVectorEntity.DENSE_VECTOR, vector, 1, 10).filter(
                                Elastic.FILTERS.eq(
                                        ESDenseVectorEntity.TEST_STRING,
                                        "FOO"
                                )
                        )
                ).first()
        )
        assertTrue {
            elastic.select(ESDenseVectorEntity::class.java).knn(
                    NearestNeighborsSearch(ESDenseVectorEntity.DENSE_VECTOR, vector, 1, 10).filter(
                            Elastic.FILTERS.eq(
                                    ESDenseVectorEntity.TEST_STRING,
                                    "BAR"
                            )
                    )
            ).first().isEmpty
        }
    }

    companion object {
        @Part
        private lateinit var elastic: Elastic
        @BeforeAll
        @JvmStatic
        fun setupSpec() {
            elastic.readyFuture.await(Duration.ofSeconds(60))
        }
    }
}
