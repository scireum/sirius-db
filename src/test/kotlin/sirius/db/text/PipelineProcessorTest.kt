/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text

import org.junit.jupiter.api.Test

/**
 * Tests the [PipelineProcessor].
 */
class PipelineProcessorTest : TokenProcessorTest() {
    private class NOOPProcessor : ChainableTokenProcessor() {
        override fun accept(token: String) {
            emit(token)
        }
    }

    @Test
    fun tokenizing() {
        assertExactTokenizing("HelloWorld", PipelineProcessor(NOOPProcessor()), "HelloWorld")
        assertExactTokenizing(
            "HelloWorld",
            PipelineProcessor(NOOPProcessor(), NOOPProcessor()),
            "HelloWorld"
        )
        assertExactTokenizing(
            "HelloWorld",
            PipelineProcessor(
                NOOPProcessor(),
                NOOPProcessor(),
                NOOPProcessor()
            ),
            "HelloWorld"
        )
        assertExactTokenizing(
            "Hello World",
            PipelineProcessor(NOOPProcessor(), NOOPProcessor()),
            "Hello World"
        )
    }
}
