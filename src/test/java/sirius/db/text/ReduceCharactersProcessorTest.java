/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Test;

public class ReduceCharactersProcessorTest extends TokenProcessorTest {

    @Test
    public void testTokenizing() {
        assertExactTokenizing("Hello", new ReduceCharacterProcessor(), "Hello");
        assertExactTokenizing("Höllo", new ReduceCharacterProcessor(), "Hoello");
        assertExactTokenizing("ÄHölloÜ", new ReduceCharacterProcessor(), "AeHoelloUe");
        assertExactTokenizing("ÄÖßÜ", new ReduceCharacterProcessor(), "AeOessUe");
        assertExactTokenizing("Hüüllüü", new ReduceCharacterProcessor(), "Hueuellueue");
        assertExactTokenizing(" Hüü llüü ", new ReduceCharacterProcessor(), " Hueue llueue ");
    }
}
