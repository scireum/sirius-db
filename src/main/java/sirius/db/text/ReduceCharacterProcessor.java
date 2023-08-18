/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import sirius.kernel.commons.StringCleanup;
import sirius.kernel.commons.Strings;

/**
 * Reduces characters to a simpler representation.
 * <p>
 * Many characters like umlauts or ligatures are "down converted" by this processor into their ASCII representation.
 */
public class ReduceCharacterProcessor extends ChainableTokenProcessor {

    @Override
    public void accept(String token) {
        emit(Strings.cleanup(token, StringCleanup::reduceCharacters));
    }
}
