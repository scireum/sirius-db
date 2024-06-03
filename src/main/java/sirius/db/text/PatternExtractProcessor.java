/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Extracts sub-tokens based on regular expressions.
 */
public class PatternExtractProcessor extends ChainableTokenProcessor {

    private static final Pattern EXTRACT_EMAILS = Pattern.compile("(\\p{Alnum}[^@]++)@(.+)$");
    /**
     * Matches numbered placeholders like {0}, {1}, {2} etc.
     */
    private static final Pattern NUMBERED_PLACEHOLDER = Pattern.compile("\\{(\\d+)}");

    private final Pattern pattern;
    private final List<List<ReplacementPattern>> replacements;

    /**
     * Creates a new processor.
     *
     * @param pattern      the pattern to match
     * @param replacements the tokens to emit if the given pattern matches. Using {0}...{N} one can reference to the
     *                     captured groups where 0 is the token itself.
     */
    public PatternExtractProcessor(Pattern pattern, String... replacements) {
        this(pattern, Stream.of(replacements));
    }

    /**
     * Creates a new processor.
     *
     * @param pattern      the pattern to match
     * @param replacements the tokens to emit if the given pattern matches. Using {0}...{N} one can reference to the
     *                     captured groups where 0 is the token itself.
     */
    public PatternExtractProcessor(Pattern pattern, Stream<String> replacements) {
        this.pattern = pattern;
        this.replacements = replacements.map(this::compileReplacementPattern).toList();
    }

    /**
     * Extracts a valid eMail addresses.
     * <p>
     * This will also emit its prefix (everything before the at character) and the host
     * (everything past the at sing).
     *
     * @return a pattern extractor which matches and processes email addresses
     */
    public static PatternExtractProcessor createEmailExtractor() {
        return new PatternExtractProcessor(EXTRACT_EMAILS, "{0}", "{1}", "{2}");
    }

    private List<ReplacementPattern> compileReplacementPattern(String input) {
        List<ReplacementPattern> result = new ArrayList<>();
        Matcher numberedPlaceholderMatcher = NUMBERED_PLACEHOLDER.matcher(input);
        int start = 0;
        while (numberedPlaceholderMatcher.find(start)) {
            if (numberedPlaceholderMatcher.start() > start) {
                result.add(new ReplacementPattern(input.substring(start, numberedPlaceholderMatcher.start())));
            }
            result.add(new ReplacementPattern(Integer.parseInt(numberedPlaceholderMatcher.group(1))));
            start = numberedPlaceholderMatcher.end();
        }

        if (start < input.length()) {
            result.add(new ReplacementPattern(input.substring(start)));
        }

        return result;
    }

    @Override
    public void accept(String value) {
        int start = 0;

        Matcher matcher = pattern.matcher(value);
        while (matcher.find(start)) {
            if (replacements.isEmpty()) {
                for (int i = 1; i < matcher.groupCount(); i++) {
                    emit(matcher.group(i));
                }
            } else {
                for (List<ReplacementPattern> replacementList : replacements) {
                    StringBuilder tokenBuilder = new StringBuilder();
                    replacementList.forEach(replacement -> replacement.execute(matcher, tokenBuilder));
                    emit(tokenBuilder.toString());
                }
            }
            start = matcher.end();
        }

        if (start == 0) {
            emit(value);
        }
    }

    private static class ReplacementPattern {
        int groupIndex = -1;
        String staticString;

        ReplacementPattern(String staticString) {
            this.staticString = staticString;
        }

        ReplacementPattern(int groupIndex) {
            this.groupIndex = groupIndex;
        }

        void execute(Matcher matcher, StringBuilder output) {
            if (groupIndex > -1) {
                output.append(matcher.group(groupIndex));
            } else {
                output.append(staticString);
            }
        }
    }
}
