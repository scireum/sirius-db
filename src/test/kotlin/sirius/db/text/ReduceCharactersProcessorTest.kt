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
 * Tests the [ReduceCharacterProcessor].
 */
class ReduceCharactersProcessorTest : TokenProcessorTest() {
    @Test
    fun tokenizing() {
        assertExactTokenizing("Hello", ReduceCharacterProcessor(), "Hello")
        assertExactTokenizing("Höllo", ReduceCharacterProcessor(), "Hoello")
        assertExactTokenizing("ÄHölloÜ", ReduceCharacterProcessor(), "AEHoelloUE")
        assertExactTokenizing("ÄÖßÜ", ReduceCharacterProcessor(), "AEOEssUE")
        assertExactTokenizing("Hüüllüü", ReduceCharacterProcessor(), "Hueuellueue")
        assertExactTokenizing(" Hüü llüü ", ReduceCharacterProcessor(), " Hueue llueue ")
    }
}
