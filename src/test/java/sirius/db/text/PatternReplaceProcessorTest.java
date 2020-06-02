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

public class PatternReplaceProcessorTest extends TokenProcessorTest {

    @Test
    public void testTokenizing() {
        assertExactTokenizing("a7b8$c", new PatternReplaceProcessor(Pattern.compile("[^a-z]"), " "), "a b  c");
        assertExactTokenizing("a7b\278$c\10\n\t", PatternReplaceProcessor.createRemoveControlCharacters(), "a7b 8$c   ");
    }
}
