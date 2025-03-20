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
 * Tests the [ToLowercaseProcessor].
 */
class ToLowercaseProcessorTest : TokenProcessorTest() {
    @Test
    fun tokenizing() {
        assertExactTokenizing("HelloWorld", ToLowercaseProcessor(), "helloworld")
        assertExactTokenizing("ÄÖÜOUKÁ", ToLowercaseProcessor(), "äöüouká")
        assertExactTokenizing("Hello World", ToLowercaseProcessor(), "hello world")
        assertExactTokenizing("123", ToLowercaseProcessor(), "123")
        assertExactTokenizing("--$--", ToLowercaseProcessor(), "--$--")
    }
}
