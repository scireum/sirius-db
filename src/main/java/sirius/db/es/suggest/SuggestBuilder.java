/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.ElasticQuery;
import sirius.db.es.constraints.BoolQueryBuilder;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Json;

/**
 * Helper class which generates term and phrase suggesters for elasticsearch which can be used via
 * {@link ElasticQuery#suggest(SuggestBuilder)}.
 *
 * @deprecated use {@link SuggestionQuery}
 */
@Deprecated(since = "2021/07/01")
public class SuggestBuilder {

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
    public SuggestBuilder(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Sets the query string to generate suggestions for and the field to generate the suggestions from
     *
     * @param field the field to get the suggestions from
     * @param text  the query to generate suggestions for
     * @return the builder itself for fluent method calls
     */
    public SuggestBuilder on(Mapping field, String text) {
        this.text = text;
        return addBodyParameter(PARAM_FIELD, field.getName());
    }

    /**
     * Adds a parameter to the body of the suggester.
     *
     * @param name  the name of the parameter
     * @param value the value of the parameter
     * @return the builder itself for fluent method calls
     */
    public SuggestBuilder addBodyParameter(String name, Object value) {
        this.body.putPOJO(name, value);
        return this;
    }

    /**
     * Helper method to set the highlighting options for phrase suggesters.
     *
     * @param preTag  the tag in front of suggested tokens
     * @param postTag the tag behind suggested tokens
     * @return the builder itself for fluent method calls
     */
    public SuggestBuilder highlight(String preTag, String postTag) {
        return addBodyParameter(PARAM_HIGHLIGHT,
                                Json.createObject().put(PARAM_PRE_TAG, preTag).put(PARAM_POST_TAG, postTag));
    }

    /**
     * Helper method to set the collate query for phrase suggesters.
     * <p>
     * The query is used to check wether at least one document in the index matches the suggestion. The template
     * parameter <pre>{{suggestion}}</pre> is replaced with the suggested text when checking.
     *
     * @param query the {@link BoolQueryBuilder}
     * @param prune <tt>true</tt> if options that didn't match the given query should remain in the response,
     *              <tt>false</tt> otherwise
     * @return the builder itself for fluent method calls
     * @see SuggestOption#isCollateMatch() for checking if a option matched the query
     */
    public SuggestBuilder collate(BoolQueryBuilder query, boolean prune) {
        return addBodyParameter(PARAM_COLLATE,
                                Json.createObject()
                                    .put(PARAM_PRUNE, prune)
                                    .set(PARAM_QUERY, Json.createObject().set(PARAM_SOURCE, query.build())));
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
