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
 * Tests the [TokenLimitProcessor].
 */
class TokenLimitProcessorTest : TokenProcessorTest() {
    @Test
    fun tokenizing() {
        assertExactTokenizing("1234567890", TokenLimitProcessor(0, 0), "1234567890")
        assertExactTokenizing("1234", TokenLimitProcessor(5, 0), *NO_TOKENS)
        assertExactTokenizing("12345", TokenLimitProcessor(5, 0), "12345")
        assertExactTokenizing("123456", TokenLimitProcessor(5, 0), "123456")
        assertExactTokenizing("123456", TokenLimitProcessor(0, 5), *NO_TOKENS)
        assertExactTokenizing("12345", TokenLimitProcessor(0, 5), "12345")
        assertExactTokenizing("1234", TokenLimitProcessor(0, 5), "1234")
    }
}
