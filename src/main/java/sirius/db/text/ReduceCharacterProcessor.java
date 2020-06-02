/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import sirius.kernel.commons.Strings;

import java.util.Map;
import java.util.TreeMap;

/**
 * Reduces characters to a simpler representation.
 * <p>
 * Many characters like umlauts or ligatures are "down converted" by this processor into their ASCII representation.
 */
public class ReduceCharacterProcessor extends ChainableTokenProcessor {

    private static final Map<Integer, String> unicodeMapping;

    static {
        unicodeMapping = new TreeMap<>();
        translateRange(0x00C0, "A", "A", "A", "A", "Ae", "A", "Ae", "C", "E", "E", "E", "E", "I", "I", "I", "I");
        translateRange(0x00D0, "D", "N", "O", "O", "O", "O", "Oe", null, null, "U", "U", "U", "Ue", "Y", null, "ss");
        translateRange(0x00E0, "a", "a", "a", "a", "ae", "a", "ae", "c", "e", "e", "e", "e", "i", "i", "i", "i");
        translateRange(0x00F0, null, "n", "o", "o", "o", "o", "oe", null, null, "u", "u", "u", "ue", "y", null, "y");

        // Latin Extended-A 0100-0170
        translateRange(0x0130, null, null, "Ij", "ij", "J", "j", "K", "k", "k", "L", "l", "L", "l", "L", "l", "L");

        // Aplphabetic Presentation Forms FB00-0FB4
        translateRange(0xFB00,
                       "ff",
                       "fi",
                       "fl",
                       "ffi",
                       "ffl",
                       "ft",
                       "st",
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null);
    }

    /**
     * Translates a range of codepoints to replacements strings.
     * <p>
     * For the given parameters, the codepoint is calculates as offset + index of
     * parameter. {@code null} means that the charater is not replaced.
     *
     * @param offset       the offset into the UNICODE table
     * @param replacements the replacements to use (starting a <tt>offset</tt>)
     */
    private static void translateRange(int offset, String... replacements) {
        int index = offset;
        for (String replacement : replacements) {
            if (replacement != null) {
                unicodeMapping.put(index, replacement);
            }
            index++;
        }
    }

    /**
     * Replaces ligatures and umlautes by their classical counterparts.
     *
     * @param term the term to replace all ligatures and umlauts in
     * @return the processed term with all ligatures and umlauts replaced.
     */
    public static String reduceCharacters(String term) {
        if (Strings.isEmpty(term)) {
            return term;
        }

        StringBuilder output = new StringBuilder();
        int lastMatch = 0;
        for (int i = 0; i < term.length(); i++) {
            String replacement = unicodeMapping.get(term.codePointAt(i));
            if (replacement != null) {
                output.append(term, lastMatch, i);
                output.append(replacement);
                lastMatch = i + 1;
            }
        }

        if (lastMatch < term.length()) {
            if (lastMatch == 0) {
                return term;
            } else {
                output.append(term.substring(lastMatch));
            }
        }

        return output.toString();
    }

    @Override
    public void accept(String token) {
        emit(reduceCharacters(token));
    }
}
