/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.HashSet;
import java.util.Set;

/**
 * Filters duplicate tokens generated by upstream processors.
 */
public class DeduplicateProcessor extends ChainableTokenProcessor {

    private Set<String> tokens = new HashSet<>();
    private boolean global;

    /**
     * Creates a new processor.
     *
     * @param global determines if deduplication happens globally (for the lifetime of the processor)
     *               or locally (until the next {@link #purge()}).
     */
    public DeduplicateProcessor(boolean global) {
        this.global = global;
    }

    @Override
    public void accept(String token) {
        if (tokens.add(token)) {
            emit(token);
        }
    }

    @Override
    public void purge() {
        if (!global) {
            tokens.clear();
        }
        super.purge();
    }
}