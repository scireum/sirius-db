/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text

import org.junit.jupiter.api.Test
import sirius.kernel.commons.Strings
import kotlin.test.assertEquals

/**
 * Tests the [BasicIndexTokenizer].
 */
class BasicIndexTokenizerTest : TokenProcessorTest() {
    @Test
    fun tokenizing() {
        assertExactTokenizing(
            "email:test@test.local",
            "[email:test@test.local, email, test, local, email:test, test.local]"
        )
        assertExactTokenizing(
            "max.mustermann@website.com",
            "[max.mustermann@website.com, max, mustermann, website, com, max.mustermann, website.com]"
        )
        assertExactTokenizing("test-foobar", "[test-foobar, test, foobar]")
        assertExactTokenizing("test123@bla-bar.foo", "[test123@bla-bar.foo, test123, bla, bar, foo, bla-bar.foo]")
    }

    private fun assertExactTokenizing(input: String?, vararg tokens: String?) {
        val tokenizer = BasicIndexTokenizer()
        val result: MutableList<String> = ArrayList()
        tokenizer.accept(
            input
        ) { outputTokens: List<String?>? ->
            result.add(
                "[" + Strings.join(outputTokens, ", ") + "]"
            )
        }
        assertEquals(listOf(*tokens), result)
    }
}
