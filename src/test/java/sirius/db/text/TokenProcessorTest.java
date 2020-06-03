/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Provides baseclass used to test <tt>TokenProcessors</tt>.
 */
abstract class TokenProcessorTest {

    public static final String[] NO_TOKENS = new String[0];

    protected static void assertExactTokenizing(String input, ChainableTokenProcessor processor, String... tokens) {
        assertExactTokenizing(Collections.singletonList(input), processor, tokens);
    }

    protected static void assertExactTokenizing(List<String> inputs,
                                                ChainableTokenProcessor processor,
                                                String... tokens) {
        List<String> result = new ArrayList<>();
        processor.chainConsumer(result::add);
        inputs.forEach(processor);
        processor.purge();
        Assert.assertEquals(Arrays.asList(tokens), result);
    }
}
