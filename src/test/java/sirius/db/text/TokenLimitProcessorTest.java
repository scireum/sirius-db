/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import org.junit.Test;

public class TokenLimitProcessorTest extends TokenProcessorTest {

    @Test
    public void testTokenizing() {
        assertExactTokenizing("1234567890", new TokenLimitProcessor(0, 0), "1234567890");
        assertExactTokenizing("1234", new TokenLimitProcessor(5, 0), NO_TOKENS);
        assertExactTokenizing("12345", new TokenLimitProcessor(5, 0), "12345");
        assertExactTokenizing("123456", new TokenLimitProcessor(5, 0), "123456");
        assertExactTokenizing("123456", new TokenLimitProcessor(0, 5), NO_TOKENS);
        assertExactTokenizing("12345", new TokenLimitProcessor(0, 5), "12345");
        assertExactTokenizing("1234", new TokenLimitProcessor(0, 5), "1234");
    }
}
