/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.function.Consumer;

/**
 * Defines the most basic interface to be provided by a processor in the token stream.
 * <p>
 * Note that almost all processors should and must be chainable and therefore be a subclass of
 * {@link ChainableTokenProcessor}.
 */
public interface TokenProcessor extends Consumer<String> {

    /**
     * Purges any internal buffered data to the next processor.
     * <p>
     * If this processor doesn't buffer any data internally, nothing will happen but any
     * subsequent processors must also be purged.
     */
    default void purge() {

    }
}
