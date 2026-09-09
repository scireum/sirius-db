/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

/**
 * Invokes {@link #purge()} after each token which is simply forwarded to the downstream processor.
 */
public class PurgeProcessor extends ChainableTokenProcessor {

    @Override
    public void accept(String token) {
        emit(token);
        purge();
    }
}
