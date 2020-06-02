/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Test;

import java.util.regex.Pattern;

public class PatternSplitProcessorTest extends TokenProcessorTest {

    @Test
    public void testTokenizing() {
        assertExactTokenizing("a7b8$c",
                              new PatternSplitProcessor(Pattern.compile("[^a-z]"), false, true),
                              "a",
                              "b",
                              "c");
        assertExactTokenizing("  A  B-6:7C-",
                              new PatternSplitProcessor(Pattern.compile("[^a-zA-Z0-9]"), true, true),
                              "  A  B-6:7C-",
                              "A",
                              "B",
                              "6",
                              "7C");
    }
}
