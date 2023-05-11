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

/**
 * Represents a suggestion for a single term of the given input text.
 */
public class TermSuggestion {

    private static final String PARAM_TEXT = "text";
    private static final String PARAM_SCORE = "score";
    private static final String PARAM_HIGHLIGHTED = "highlighted";
    private static final String PARAM_COLLATE_MATCH = "collate_match";

    private String text;
    private String highlighted;
    private float score;
    private boolean collateMatch;

    /**
     * Generates a new suggestion based on the JSON returned by Elasticsearch.
     *
     * @param json a JSON snipped as returned by ES
     */
    public TermSuggestion(ObjectNode json) {
        this.text = Json.tryValueString(json, PARAM_TEXT).orElse(null);
        this.score = json.path(PARAM_SCORE).floatValue();
        this.highlighted = Json.tryValueString(json, PARAM_HIGHLIGHTED).orElse(null);
        this.collateMatch = json.path(PARAM_COLLATE_MATCH).asBoolean();
    }

    /**
     * Return the suggested text of this option.
     *
     * @return the suggested text
     */
    public String getText() {
        return text;
    }

    /**
     * Return the highlighted suggested text of this option.
     *
     * @return the highlighted suggested text
     */
    public String getHighlighted() {
        return highlighted;
    }

    /**
     * Returns the score of this option.
     *
     * @return the score
     */
    public float getScore() {
        return score;
    }

    /**
     * Returns whether this option is a match to the collate query of a phrase suggester.
     *
     * @return <tt>true</tt> if this option matched the collate query, <tt>false</tt> otherwise
     */
    public boolean isCollateMatch() {
        return collateMatch;
    }
}
