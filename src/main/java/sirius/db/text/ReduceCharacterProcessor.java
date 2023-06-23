/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import sirius.kernel.commons.Strings;

import java.util.Set;

/**
 * Reduces characters to a simpler representation.
 * <p>
 * Many characters like umlauts or ligatures are "down converted" by this processor into their ASCII representation.
 */
public class ReduceCharacterProcessor extends ChainableTokenProcessor {

    /**
     * Replaces ligatures and umlautes by their classical counterparts.
     *
     * @param term the term to replace all ligatures and umlauts in
     * @return the processed term with all ligatures and umlauts replaced.
     * @deprecated Use {@link Strings#reduceCharacters(String)} or {@link Strings#cleanup(String, Set)} instead.
     */
    @Deprecated
    public static String reduceCharacters(String term) {
        return Strings.reduceCharacters(term);
    }

    @Override
    public void accept(String token) {
        emit(Strings.reduceCharacters(token));
    }
}
