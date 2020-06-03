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

public class PatternExtractProcessorTest extends TokenProcessorTest {

    @Test
    public void testTokenizing() {
        assertExactTokenizing("ABC 85X 84 77X",
                              new PatternExtractProcessor(Pattern.compile("(\\d+)X"), "{1}", "A{1}B", "A {1} B", "C"),
                              "85",
                              "A85B",
                              "A 85 B",
                              "C",
                              "77",
                              "A77B",
                              "A 77 B",
                              "C");
    }
}
