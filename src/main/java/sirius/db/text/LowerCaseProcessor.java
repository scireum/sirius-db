/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

/**
 * Converts all tokens to lowercase.
 */
public class LowerCaseProcessor extends ChainableTokenProcessor {

    @Override
    public void accept(String token) {
        emit(token.toLowerCase());
    }
}
