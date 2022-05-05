/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.alibaba.fastjson.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a part in an Elasticsearch suggest response.
 *
 * @deprecated use {@link SuggestionQuery}
 */
@Deprecated(since = "2021/07/01")
public class SuggestPart {

    private static final String PARAM_TEXT = "text";
    private static final String PARAM_OFFSET = "offset";
    private static final String PARAM_LENGTH = "length";
    private static final String PARAM_OPTIONS = "options";

    private String text;
    private int offset;
    private int length;

    private List<SuggestOption> options;

    /**
     * Creates a new suggest part with the given original text and position.
     *
     * @param text   the original text of this part
     * @param offset the start of this part
     * @param length the length of this part
     */
    public SuggestPart(String text, int offset, int length) {
        this.text = text;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Returns the original text of this part.
     *
     * @return the original text
     */
    public String getText() {
        return text;
    }

    /**
     * Returns the start of this part.
     *
     * @return the start
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Returns the length of this part.
     *
     * @return the length
     */
    public int getLength() {
        return length;
    }

    /**
     * Returns the options given for this part.
     *
     * @return a list of {@link SuggestOption}s
     */
    public List<SuggestOption> getOptions() {
        if (options == null) {
            return Collections.emptyList();
        }

        return options;
    }

    /**
     * Adds the given {@link SuggestOption}s to this part.
     *
     * @param options a list of suggest options to add to this part
     * @return the suggest part itself for fluent method calls
     */
    public SuggestPart withOptions(List<SuggestOption> options) {
        this.options = options;
        return this;
    }

    /**
     * Build a suggest part from the given {@link JSONObject}.
     *
     * @param part the JSON object
     * @return the newly created suggest part
     */
    public static SuggestPart makeSuggestPart(JSONObject part) {
        SuggestPart suggestPart = new SuggestPart(part.getString(PARAM_TEXT),
                                                  part.getIntValue(PARAM_OFFSET),
                                                  part.getIntValue(PARAM_LENGTH));

        suggestPart.withOptions(part.getJSONArray(PARAM_OPTIONS)
                                    .stream()
                                    .map(JSONObject.class::cast)
                                    .map(SuggestOption::makeSuggestOption)
                                    .collect(Collectors.toList()));

        return suggestPart;
    }
}
