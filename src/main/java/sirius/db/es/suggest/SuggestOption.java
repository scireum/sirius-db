/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.alibaba.fastjson.JSONObject;

/**
 * Represents an option in an Elasticsearch suggest response.
 */
public class SuggestOption {

    private static final String PARAM_TEXT = "text";
    private static final String PARAM_SCORE = "score";
    private static final String PARAM_HIGHLIGHTED = "highlighted";
    private static final String PARAM_COLLATE_MATCH = "collate_match";

    private String text;
    private String highlighted = "";
    private float score;
    private boolean collateMatch = true;

    /**
     * Creates a new suggest option with the suggested text and it's score.
     *
     * @param text  the suggested text
     * @param score the score of the option
     */
    public SuggestOption(String text, float score) {
        this.text = text;
        this.score = score;
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
     * Sets the highlighted version of the suggested text.
     *
     * @param highlighted the highlighted suggested text
     * @return the suggest option itself for fluent method calls
     */
    public SuggestOption withHighlighted(String highlighted) {
        this.highlighted = highlighted;
        return this;
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
     * Returns wether this option is a match to the collate query of a phrase suggester.
     *
     * @return <tt>true</tt> if this option matched the collate query, <tt>false</tt> otherwise
     */
    public boolean isCollateMatch() {
        return collateMatch;
    }

    /**
     * Sets wether this option matched the collate query.
     *
     * @param collateMatch <tt>true</tt> if this option matched the collate query, <tt>false</tt> otherwise
     * @return the suggest option itself for fluent method calls
     */
    public SuggestOption withCollateMatch(boolean collateMatch) {
        this.collateMatch = collateMatch;
        return this;
    }

    /**
     * Build a suggest option from the given {@link JSONObject}.
     *
     * @param option the JSON object
     * @return the newly created suggest option
     */
    public static SuggestOption makeSuggestOption(JSONObject option) {
        SuggestOption suggestOption =
                new SuggestOption(option.getString(PARAM_TEXT), option.getFloatValue(PARAM_SCORE));

        if (option.containsKey(PARAM_HIGHLIGHTED)) {
            suggestOption.withHighlighted(option.getString(PARAM_HIGHLIGHTED));
        }

        if (option.containsKey(PARAM_COLLATE_MATCH)) {
            suggestOption.withCollateMatch(option.getBooleanValue(PARAM_COLLATE_MATCH));
        }

        return suggestOption;
    }
}
