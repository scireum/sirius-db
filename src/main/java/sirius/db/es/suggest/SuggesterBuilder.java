/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticQuery;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Json;

/**
 * Helper class which generates term and phrase suggesters for Elasticsearch which can be used via
 * {@link Elastic#suggest(Class)}.
 */
public class SuggesterBuilder {

    public static final String TYPE_TERM = "term";
    public static final String TYPE_PHRASE = "phrase";

    private static final String PARAM_TEXT = "text";
    private static final String PARAM_FIELD = "field";
    private static final String PARAM_HIGHLIGHT = "highlight";
    private static final String PARAM_PRE_TAG = "pre_tag";
    private static final String PARAM_POST_TAG = "post_tag";
    private static final String PARAM_COLLATE = "collate";
    private static final String PARAM_QUERY = "query";
    private static final String PARAM_PRUNE = "prune";
    private static final String PARAM_SOURCE = "source";
    private static final String PARAM_SIZE = "size";
    private static final String PARAM_SUGGEST_MODE = "suggest_mode";

    /**
     * Only provide suggestions for suggest text terms that are not in the index. This is the default.
     */
    public static final String SUGGEST_MODE_MISSING = "missing";

    /**
     * Only suggest suggestions that occur in more docs than the original suggest text term.
     */
    public static final String SUGGEST_MODE_POPULAR = "popular";

    /**
     * Suggest any matching suggestions based on terms in the suggest text.
     */
    public static final String SUGGEST_MODE_ALWAYS = "always";

    private String type;
    private String name;
    private String text;

    private ObjectNode body = Json.createObject();

    /**
     * Creates a new suggest builder.
     *
     * @param type the type of the suggester.
     * @param name the name of the suggester
     */
    public SuggesterBuilder(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Sets the query string to generate suggestions for and the field to generate the suggestions from.
     *
     * @param field the field to get the suggestions from
     * @return the builder itself for fluent method calls
     */
    public SuggesterBuilder on(Mapping field) {
        return withBodyParameter(PARAM_FIELD, field.getName());
    }

    /**
     * Sets the query string to generate suggestions for and the field to generate the suggestions from.
     *
     * @param text the query to generate suggestions for
     * @return the builder itself for fluent method calls
     */
    public SuggesterBuilder forText(String text) {
        this.text = text;
        return this;
    }

    /**
     * Adds a parameter to the body of the suggester.
     *
     * @param name  the name of the parameter
     * @param value the value of the parameter
     * @return the builder itself for fluent method calls
     */
    public SuggesterBuilder withBodyParameter(String name, Object value) {
        this.body.putPOJO(name, value);
        return this;
    }

    /**
     * Specifies the number of suggestions to generate for a {@link #TYPE_TERM} suggester.
     *
     * @param numberOfSuggestions specifies the number of suggestions per term
     * @return the builder itself for fluent method calls
     */
    public SuggesterBuilder withSize(int numberOfSuggestions) {
        this.body.put(PARAM_SIZE, numberOfSuggestions);
        return this;
    }

    /**
     * Specifies mode to use when generating suggestions for a {@link #TYPE_TERM} suggester.
     *
     * @param suggestMode the suggest mode to use
     * @return the builder itself for fluent method calls
     */
    public SuggesterBuilder withSuggestMode(String suggestMode) {
        this.body.put(PARAM_SUGGEST_MODE, suggestMode);
        return this;
    }

    /**
     * Helper method to set the highlighting options for phrase suggesters.
     *
     * @param preTag  the tag in front of suggested tokens
     * @param postTag the tag behind suggested tokens
     * @return the builder itself for fluent method calls
     */
    public SuggesterBuilder highlight(String preTag, String postTag) {
        return withBodyParameter(PARAM_HIGHLIGHT,
                                 Json.createObject().put(PARAM_PRE_TAG, preTag).put(PARAM_POST_TAG, postTag));
    }

    /**
     * Helper method to set the collate query for phrase suggesters.
     * <p>
     * The query is used to check whether at least one document in the <b>CURRENT SHARD</b> matches the suggestion.
     * The template parameter <pre>{{suggestion}}</pre> is replaced with the suggested text when checking.
     * <p>
     * Note that this is rather useful to check if the phrase itself is a match than to check if the suggested
     * phrase produces a search result using a complex query.
     *
     * @param query a JSON object representing the filters to apply
     * @param prune <tt>true</tt> if options that didn't match the given query should remain in the response,
     *              <tt>false</tt> otherwise
     * @return the builder itself for fluent method calls
     * @see TermSuggestion#isCollateMatch() for checking if a option matched the query
     */
    public SuggesterBuilder collate(ObjectNode query, boolean prune) {
        return withBodyParameter(PARAM_COLLATE,
                                 Json.createObject()
                                     .put(PARAM_PRUNE, prune)
                                     .set(PARAM_QUERY, Json.createObject().set(PARAM_SOURCE, query)));
    }

    /**
     * Uses the given query for collation.
     * <p>
     * Most probably this will use a base query, create a copy and add an additional constraint like:
     * {@code .collate(query.copy().eq(SOME_FIELD, "{{suggestion}}")}.
     *
     * @param query the query to derive the fitlers from
     * @param prune <tt>true</tt> if options that didn't match the given query should remain in the response,
     *              <tt>false</tt> otherwise
     * @return the builder itself for fluent method calls
     * @see TermSuggestion#isCollateMatch() for checking if a option matched the query
     */
    public SuggesterBuilder collate(ElasticQuery<?> query, boolean prune) {
        return collate(query.getFilters(), prune);
    }

    /**
     * Generates an {@link ObjectNode} that represents this suggester.
     *
     * @return the suggester as a JSON object
     */
    public ObjectNode build() {
        return Json.createObject().put(PARAM_TEXT, text).set(type, body);
    }
}
