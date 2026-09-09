/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

/**
 * Converts all tokens to lowercase.
 */
public class ToLowercaseProcessor extends ChainableTokenProcessor {

    @Override
    public void accept(String token) {
        emit(token.toLowerCase());
    }
}
