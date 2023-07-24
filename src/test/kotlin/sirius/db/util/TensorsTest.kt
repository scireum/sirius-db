/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.util

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

/**
 * Tests the {@link Tensors} class.
 */
class TensorsTest {
    @Test
    fun `test string tools`() {
        val tensor = floatArrayOf(1f, 2f, 3f, 4f, 5f)

        Assertions.assertArrayEquals(Tensors.parse(Tensors.encode(tensor)), tensor)
    }

    @Test
    fun `test array tools`() {
        val tensor = floatArrayOf(1f, 2f, 3f, 4f, 5f)

        Assertions.assertArrayEquals(Tensors.fromList(tensor.asList()), tensor)
    }

}
