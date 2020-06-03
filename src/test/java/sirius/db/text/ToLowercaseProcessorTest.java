/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Test;

public class ToLowercaseProcessorTest extends TokenProcessorTest {

    @Test
    public void testTokenizing() {
        assertExactTokenizing("HelloWorld", new ToLowercaseProcessor(), "helloworld");
        assertExactTokenizing("ÄÖÜOUKÁ", new ToLowercaseProcessor(), "äöüouká");
        assertExactTokenizing("Hello World", new ToLowercaseProcessor(), "hello world");
        assertExactTokenizing("123", new ToLowercaseProcessor(), "123");
        assertExactTokenizing("--$--", new ToLowercaseProcessor(), "--$--");
    }
}
