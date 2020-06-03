/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Extracts sub-tokens based on regular expressions.
 */
public class PatternExtractProcessor extends ChainableTokenProcessor {

    private static final Pattern EXTRACT_EMAILS = Pattern.compile("(\\p{Alnum}.+)@(.+)$");

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

    private Pattern pattern;
    private List<List<ReplacementPattern>> replacements;

    /**
     * Creates a new processor.
     *
     * @param pattern      the pattern to match
     * @param replacements the tokens to emit if the given pattern matches. Using {0}...{N} one can reference to the
     *                     captured groups where 0 is the token itself.
     */
    public PatternExtractProcessor(Pattern pattern, String... replacements) {
        this.pattern = pattern;
        this.replacements =
                Arrays.stream(replacements).map(this::compileReplacementPattern).collect(Collectors.toList());
    }

    public static PatternExtractProcessor createEmailExtractor() {
        return new PatternExtractProcessor(EXTRACT_EMAILS, "{0}","{1}","{2}");
    }

    private List<ReplacementPattern> compileReplacementPattern(String input) {
        List<ReplacementPattern> result = new ArrayList<>();
        Matcher matcher = Pattern.compile("\\{(\\d+)}").matcher(input);
        int start = 0;
        while (matcher.find(start)) {
            if (matcher.start() > start) {
                result.add(new ReplacementPattern(input.substring(start, matcher.start())));
            }
            result.add(new ReplacementPattern(Integer.parseInt(matcher.group(1))));
            start = matcher.end();
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
}
