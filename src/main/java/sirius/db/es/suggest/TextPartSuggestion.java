/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.alibaba.fastjson2.JSONObject;

import java.util.Collections;
import java.util.List;

/**
 * Represents a list of suggestions for a piece of the given input text.
 */
public class TextPartSuggestion {

    private static final String PARAM_TEXT = "text";
    private static final String PARAM_OFFSET = "offset";
    private static final String PARAM_LENGTH = "length";
    private static final String PARAM_OPTIONS = "options";

    private String text;
    private int offset;
    private int length;
    private List<TermSuggestion> termSuggestions;

    /**
     * Creates a new phrase suggestion based on JSON as returned by Elasticsearch.
     *
     * @param json a JSON snipped as returned by ES
     */
    public TextPartSuggestion(JSONObject json) {
        this.text = json.getString(PARAM_TEXT);
        this.offset = json.getIntValue(PARAM_OFFSET);
        this.length = json.getIntValue(PARAM_LENGTH);
        this.termSuggestions =
                json.getJSONArray(PARAM_OPTIONS).stream().map(JSONObject.class::cast).map(TermSuggestion::new).toList();
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
     * Returns the suggestions given for this part.
     *
     * @return a list of {@link TermSuggestion term suggestions}
     */
    public List<TermSuggestion> getTermSuggestions() {
        return Collections.unmodifiableList(termSuggestions);
    }
}
