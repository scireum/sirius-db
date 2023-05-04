/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.kernel.commons.Json;

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
    public TextPartSuggestion(ObjectNode json) {
        this.text = json.get(PARAM_TEXT).asText();
        this.offset = json.get(PARAM_OFFSET).asInt();
        this.length = json.get(PARAM_LENGTH).asInt();
        this.termSuggestions = Json.streamEntries(Json.getArray(json, PARAM_OPTIONS))
                                   .map(ObjectNode.class::cast)
                                   .map(TermSuggestion::new)
                                   .toList();
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
