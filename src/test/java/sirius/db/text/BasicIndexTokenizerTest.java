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
import java.util.Collections;
import java.util.List;

public class BasicIndexTokenizerTest extends TokenProcessorTest {

    protected static void assertExactTokenizing(List<String> inputs, String... tokens) {
        Watch w = Watch.start();
        BasicIndexTokenizer tokenizer = new BasicIndexTokenizer();
        List<String> result = new ArrayList<>();
        tokenizer.accept(inputs, outputTokens -> result.add("[" + Strings.join(outputTokens, ", ") + "]"));
        Assert.assertEquals(Arrays.asList(tokens), result);
    }

    @Test
    public void testVariousTokenizationPatterns() {
        assertExactTokenizing(Collections.singletonList("email:test@test.local"),
                              "[email:test@test.local, email, test, local, email:test, test.local]");
        assertExactTokenizing(Collections.singletonList("max.mustermann@website.com"),
                              "[max.mustermann@website.com, max, mustermann, website, com, max.mustermann, website.com]");
        assertExactTokenizing(Collections.singletonList("test-foobar"), "[test-foobar, test, foobar]");
        assertExactTokenizing(Collections.singletonList("test123@bla-bar.foo"),
                              "[test123@bla-bar.foo, test123, bla, bar, foo, bla-bar.foo]");
    }
}
