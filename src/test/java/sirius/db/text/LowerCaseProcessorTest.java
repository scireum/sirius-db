/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Test;

public class LowerCaseProcessorTest extends TokenProcessorTest {

    @Test
    public void testTokenizing() {
        assertExactTokenizing("HelloWorld", new LowerCaseProcessor(), "helloworld");
        assertExactTokenizing("ÄÖÜOUKÁ", new LowerCaseProcessor(), "äöüouká");
        assertExactTokenizing("Hello World", new LowerCaseProcessor(), "hello world");
        assertExactTokenizing("123", new LowerCaseProcessor(), "123");
        assertExactTokenizing("--$--", new LowerCaseProcessor(), "--$--");
    }
}
