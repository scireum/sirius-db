/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import sirius.kernel.commons.Monoflop;

import java.util.regex.Pattern;

/**
 * Splits tokens using a regular expression.
 */
public class PatternSplitProcessor extends ChainableTokenProcessor {

    private static final Pattern HARD_BOUNDARY = Pattern.compile("[^\\p{L}\\d_\\-.,:/\\\\@ ]");
    private static final Pattern SOFT_BOUNDARY = Pattern.compile("[^\\p{L}\\d]");
    private static final Pattern WHITESPACE = Pattern.compile("\\p{javaWhitespace}");

    private Pattern pattern;
    private boolean preserveOriginal;
    private boolean purge;

    /**
     * Creates a new processor.
     *
     * @param pattern          the pattern used to split at
     * @param preserveOriginal determines if the original token should also be emitted
     * @param purge            determines if a {@link #purge()} should be invoked after each split
     */
    public PatternSplitProcessor(Pattern pattern, boolean preserveOriginal, boolean purge) {
        this.pattern = pattern;
        this.preserveOriginal = preserveOriginal;
        this.purge = purge;
    }

    /**
     * Creates a processor which splits a hard "word" boundaries.
     * <p>
     * This are all punctation symbols other than <tt>/ , . : \ _ -</tt>
     *
     * @return a new processor which splits at hard token boundaries. This will {@link #purge()} after each sub token
     * being emitted
     */
    public static PatternSplitProcessor createHardBoundarySplitter() {
        return new PatternSplitProcessor(HARD_BOUNDARY, false, true);
    }

    /**
     * Creates a processor which splits at whitespace characters.
     * <p>
     * These are all characters for which <tt>Character.isWhitespace</tt> returns <tt>true</tt>.
     *
     * @return a new processor which splits a whitespaces.
     */
    public static PatternSplitProcessor createWhitespaceSplitter() {
        return new PatternSplitProcessor(WHITESPACE, false, false);
    }

    /**
     * Creates a processor which splits at soft boundaries.
     * <p>
     * These are all characters which are neither letters nor digits.
     *
     * @return a new processor which at any character which is neither a letter nor a digit. This will also preserve the
     * original (unsplitted) token.
     */
    public static PatternSplitProcessor createSoftBoundarySplitter() {
        return new PatternSplitProcessor(SOFT_BOUNDARY, true, false);
    }

    @Override
    public void accept(String token) {
        if (preserveOriginal) {
            emit(token);
            if (purge) {
                purge();
            }
        }

        Monoflop mf = Monoflop.create();
        pattern.splitAsStream(token).forEach(subToken -> {
            if (mf.successiveCall() && purge) {
                purge();
            }
            emit(subToken);
        });
    }
}
