/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Test;

public class PipelineProcessorTest extends TokenProcessorTest {

    private static class NOOPProcessor extends ChainableTokenProcessor {

        @Override
        public void accept(String token) {
            emit(token);
        }
    }

    @Test
    public void testTokenizing() {
        assertExactTokenizing("HelloWorld", new PipelineProcessor(new NOOPProcessor()), "HelloWorld");
        assertExactTokenizing("HelloWorld",
                              new PipelineProcessor(new NOOPProcessor(), new NOOPProcessor()),
                              "HelloWorld");
        assertExactTokenizing("HelloWorld",
                              new PipelineProcessor(new NOOPProcessor(),
                                                    new NOOPProcessor(),
                                                    new NOOPProcessor()),
                              "HelloWorld");
        assertExactTokenizing("Hello World",
                              new PipelineProcessor(new NOOPProcessor(), new NOOPProcessor()),
                              "Hello World");
    }
}
