/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.regex.Pattern;

/**
 * Replaces characters using a regular expression.
 */
public class PatternReplaceProcessor extends ChainableTokenProcessor {

    private static final Pattern CONTROL_CHARACTERS = Pattern.compile("\\p{Cntrl}");

    private Pattern pattern;
    private String replacement;

    /**
     * Creates a new processor.
     *
     * @param pattern     the pattern to match
     * @param replacement the replacement to use a definec by {@link java.util.regex.Matcher#replaceAll(String)}
     */
    public PatternReplaceProcessor(Pattern pattern, String replacement) {
        this.pattern = pattern;
        this.replacement = replacement;
    }

    /**
     * Creates a new processor which replaces all control characters by whitespaces.
     *
     * @return the new processor which replaces all ANSI control characters.
     */
    public static PatternReplaceProcessor createRemoveControlCharacters() {
        return new PatternReplaceProcessor(CONTROL_CHARACTERS, " ");
    }

    @Override
    public void accept(String token) {
        emit(pattern.matcher(token).replaceAll(replacement));
    }
}
