/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text

import org.junit.jupiter.api.Test
import java.util.regex.Pattern

/**
 * Tests the [PatternReplaceProcessor].
 */
class PatternReplaceProcessorTest : TokenProcessorTest() {
    @Test
    fun tokenizing() {
        assertExactTokenizing("a7b8\$c", PatternReplaceProcessor(Pattern.compile("[^a-z]"), " "), "a b  c")
        assertExactTokenizing(
            "a7b\u00178\$c\u0008\n\t",
            PatternReplaceProcessor.createRemoveControlCharacters(),
            "a7b 8\$c   "
        )
    }
}
