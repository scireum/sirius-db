/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text

import kotlin.test.assertEquals

/**
 * Provides baseclass used to test <tt>TokenProcessors</tt>.
 */
abstract class TokenProcessorTest {
    companion object {
        val NO_TOKENS: Array<String?> = arrayOfNulls(0)
    }

    protected fun assertExactTokenizing(
        input: String,
        processor: ChainableTokenProcessor,
        vararg tokens: String?
    ) {
        val result: MutableList<String> = ArrayList()
        processor.chainConsumer { token: String -> result.add(token) }
        listOf(input).forEach(processor)
        processor.purge()
        assertEquals(listOf(*tokens), result)
    }
}
