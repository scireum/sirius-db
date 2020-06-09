/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Assert;
import org.junit.Test;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Watch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BasicIndexTokenizerTest extends TokenProcessorTest {

    protected static void assertExactTokenizing(String input, String... tokens) {
        Watch w = Watch.start();
        BasicIndexTokenizer tokenizer = new BasicIndexTokenizer();
        List<String> result = new ArrayList<>();
        tokenizer.accept(input, outputTokens -> result.add("[" + Strings.join(outputTokens, ", ") + "]"));
        Assert.assertEquals(Arrays.asList(tokens), result);
    }

    @Test
    public void testVariousTokenizationPatterns() {
        assertExactTokenizing("email:test@test.local",
                              "[email:test@test.local, email, test, local, email:test, test.local]");
        assertExactTokenizing("max.mustermann@website.com",
                              "[max.mustermann@website.com, max, mustermann, website, com, max.mustermann, website.com]");
        assertExactTokenizing("test-foobar", "[test-foobar, test, foobar]");
        assertExactTokenizing("test123@bla-bar.foo", "[test123@bla-bar.foo, test123, bla, bar, foo, bla-bar.foo]");
    }
}
