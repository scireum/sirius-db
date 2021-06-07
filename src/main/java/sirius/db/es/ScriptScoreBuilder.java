/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import sirius.kernel.commons.Strings;

/**
 * Helper class which generates a script score query for elasticsearch which can be used via
 * {@link ElasticQuery#scriptScore(ScriptScoreBuilder)}.
 */
public class ScriptScoreBuilder {

    private static final String SCRIPT_SCORE = "script_score";
    private static final String FIELD_QUERY = "query";
    private static final String FIELD_SCRIPT = "script";
    private static final String FIELD_SOURCE = "source";

    private String source;

    /**
     * Adds a script function which uses the given script to return the score to use.
     *
     * @param source the script source as string
     * @return the builder itself for fluent method calls
     */
    public ScriptScoreBuilder source(String source) {
        this.source = source;
        return this;
    }

    /**
     * Adds a random score source with the given start seed.
     *
     * @param startSeed the field to read
     * @return the builder itself for fluent method calls
     */
    public ScriptScoreBuilder randomScore(int startSeed) {
        return source(Strings.apply("randomScore(%s)", startSeed));
    }

    /**
     * Applies the given query to the function score query and returns the newly created query.
     *
     * @param query the query to apply
     * @return the function score query as {@link JSONObject}
     */
    public JSONObject apply(JSONObject query) {
        return new JSONObject().fluentPut(SCRIPT_SCORE,
                                          new JSONObject().fluentPut(FIELD_QUERY, query)
                                                          .fluentPut(FIELD_SCRIPT,
                                                                     new JSONObject().fluentPut(FIELD_SOURCE, source)));
    }

    /**
     * Builds the function score query.
     *
     * @return the function score query as {@link JSONObject}
     */
    public JSONObject build() {
        return new JSONObject().fluentPut(SCRIPT_SCORE,
                                          new JSONObject().fluentPut(FIELD_SCRIPT,
                                                                     new JSONObject().fluentPut(FIELD_SOURCE, source)));
    }

    /**
     * Generates a copy of this function score builder to support {@link ElasticQuery#copy()}.
     *
     * @return a copy of this builder
     */
    public ScriptScoreBuilder copy() {
        ScriptScoreBuilder copy = new ScriptScoreBuilder();
        copy.source = source;
        return copy;
    }
}
