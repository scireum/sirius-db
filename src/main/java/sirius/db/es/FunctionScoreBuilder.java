/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Strings;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class which generates a function score query for elasticsearch which can be used via
 * {@link ElasticQuery#functionScore(FunctionScoreBuilder)}.
 */
public class FunctionScoreBuilder {

    private static final String FUNCTION_SCORE = "function_score";
    private static final String FIELD_QUERY = "query";
    private static final String FIELD_FUNCTIONS = "functions";
    private static final String FUNCTION_FIELD_VALUE_FACTOR = "field_value_factor";
    private static final String FIELD_FIELD = "field";
    private static final String FIELD_FACTOR = "factor";
    private static final String FIELD_MISSING = "missing";
    private static final String FIELD_ORIGIN = "origin";
    private static final String FIELD_SCALE = "scale";
    private static final String FIELD_OFFSET = "offset";
    private static final String FIELD_DECAY = "decay";
    private static final String DECAY_TYPE_LINEAR = "linear";
    private static final String DECAY_TYPE_GAUSS = "gauss";
    private static final String DECAY_TYPE_EXP = "exp";
    private static final String SUFFIX_SECONDS = "s";

    /**
     * Provides a re-usable function score builder to generate a random score.
     * <p>
     * Note that this isn't public, as these builders are inherently mutable and this very dangerous to share.
     */
    protected static final FunctionScoreBuilder RANDOM_SCORE = new FunctionScoreBuilder().random().replaceScore();

    private Map<String, Object> parameters = new HashMap<>();
    private List<JSONObject> functions = new ArrayList<>();

    /**
     * Adds the given parameter to the query.
     *
     * @param name  the name of the parameter
     * @param value the value of the parameter
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder parameter(String name, Object value) {
        parameters.put(name, value);
        return this;
    }

    /**
     * Adds the given function to the query.
     *
     * @param function the function as {@link JSONObject}
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder function(JSONObject function) {
        functions.add(function);
        return this;
    }

    /**
     * Replaces the score of each document by the score computed by this function score.
     * <p>
     * The default is to multiply them. However, if no other score is applied, the document score is always 0.0
     *
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder replaceScore() {
        return parameter("boost_mode", "replace");
    }

    /**
     * Computes a random score.
     *
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder random() {
        functions.add(new JSONObject().fluentPut("random_score", new JSONObject()));
        return this;
    }

    /**
     * Adds a script function which uses the given script to return the score to use.
     *
     * @param script the script used to compute the score
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder script(String script) {
        return function(new JSONObject().fluentPut("script_score",
                                                   new JSONObject().fluentPut("script",
                                                                              new JSONObject().fluentPut("source",
                                                                                                         script))));
    }

    /**
     * Adds a score function which simply reads the given field.
     *
     * @param field   the field to read
     * @param factor  the factor to apply (multiply with)
     * @param missing the value to use in case the given field is empty
     * @return the builder itself for fluent method calls
     * @see #maxFieldValueFunction(Mapping, float) if the value needs to be limited by a lower bound
     */
    public FunctionScoreBuilder fieldValueFunction(Mapping field, float factor, float missing) {
        return function(new JSONObject().fluentPut(FUNCTION_FIELD_VALUE_FACTOR,
                                                   new JSONObject().fluentPut(FIELD_FIELD, field.toString())
                                                                   .fluentPut(FIELD_FACTOR, factor)
                                                                   .fluentPut(FIELD_MISSING, missing)));
    }

    /**
     * Adds a computed function which uses a field value which is limited to the given lower level.
     *
     * @param field    the field to read
     * @param minValue the minimal value to apply in case the field value is lower
     * @return the max value of either the field or the given lower limit
     * @see #fieldValueFunction(Mapping, float, float)
     */
    public FunctionScoreBuilder maxFieldValueFunction(Mapping field, float minValue) {
        return script(Strings.apply("Math.max(%2$s, doc['%1$s'].size() == 0 ? %2$s : doc['%1$s'].value)", field, minValue));
    }

    private FunctionScoreBuilder dateTimeDecayFunction(String function,
                                                       Mapping field,
                                                       LocalDateTime origin,
                                                       Duration scale,
                                                       Duration offset,
                                                       float decay) {
        JSONObject settings = new JSONObject().fluentPut(FIELD_ORIGIN,
                                                         DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(origin.atZone(
                                                                 ZoneId.systemDefault())))
                                              .fluentPut(FIELD_SCALE, scale.getSeconds() + SUFFIX_SECONDS)
                                              .fluentPut(FIELD_OFFSET, offset.getSeconds() + SUFFIX_SECONDS)
                                              .fluentPut(FIELD_DECAY, decay);
        return function(new JSONObject().fluentPut(function, new JSONObject().fluentPut(field.toString(), settings)));
    }

    /**
     * Adds a linear decay for the given date/time field.
     * <p>
     * For further information see:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html
     *
     * @param field  the field to read
     * @param origin the origin to compute the computation from. This should most probably be "now".
     * @param scale  the distance from origin + offset at which the computed score will equal decay parameter.
     * @param offset if an offset is defined, the decay function will only compute the decay function for documents
     *               with a distance greater than the defined offset
     * @param decay  defines how documents are scored at the distance given at scale
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder linearDateTimeDecayFunction(Mapping field,
                                                            LocalDateTime origin,
                                                            Duration scale,
                                                            Duration offset,
                                                            float decay) {
        return dateTimeDecayFunction(DECAY_TYPE_LINEAR, field, origin, scale, offset, decay);
    }

    /**
     * Adds a gauss shaped decay for the given date/time field.
     * <p>
     * For further information see:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html
     *
     * @param field  the field to read
     * @param origin the origin to compute the computation from. This should most probably be "now".
     * @param scale  the distance from origin + offset at which the computed score will equal decay parameter.
     * @param offset if an offset is defined, the decay function will only compute the decay function for documents
     *               with a distance greater than the defined offset
     * @param decay  defines how documents are scored at the distance given at scale
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder gaussDateTimeDecayFunction(Mapping field,
                                                           LocalDateTime origin,
                                                           Duration scale,
                                                           Duration offset,
                                                           float decay) {
        return dateTimeDecayFunction(DECAY_TYPE_GAUSS, field, origin, scale, offset, decay);
    }

    /**
     * Adds an expotential/hyperbolic decay for the given date/time field.
     * <p>
     * For further information see:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html
     *
     * @param field  the field to read
     * @param origin the origin to compute the computation from. This should most probably be "now".
     * @param scale  the distance from origin + offset at which the computed score will equal decay parameter.
     * @param offset if an offset is defined, the decay function will only compute the decay function for documents
     *               with a distance greater than the defined offset
     * @param decay  defines how documents are scored at the distance given at scale
     * @return the builder itself for fluent method calls
     */
    public FunctionScoreBuilder expDateTimeDecayFunction(Mapping field,
                                                         LocalDateTime origin,
                                                         Duration scale,
                                                         Duration offset,
                                                         float decay) {
        return dateTimeDecayFunction(DECAY_TYPE_EXP, field, origin, scale, offset, decay);
    }

    /**
     * Applies the given query to the function score query and returns the newly created query.
     *
     * @param query the query to apply
     * @return the function score query as {@link JSONObject}
     */
    public JSONObject apply(JSONObject query) {
        return new JSONObject().fluentPut(FUNCTION_SCORE,
                                          new JSONObject().fluentPut(FIELD_QUERY, query)
                                                          .fluentPut(FIELD_FUNCTIONS,
                                                                     new JSONArray(new ArrayList<>(functions)))
                                                          .fluentPutAll(parameters));
    }

    /**
     * Builds the function score query.
     *
     * @return the function score query as {@link JSONObject}
     */
    public JSONObject build() {
        return new JSONObject().fluentPut(FUNCTION_SCORE,
                                          new JSONObject().fluentPut(FIELD_FUNCTIONS,
                                                                     new JSONArray(new ArrayList<>(functions)))
                                                          .fluentPutAll(parameters));
    }

    /**
     * Generates a copy of this function score builder to support {@link ElasticQuery#copy()}.
     *
     * @return a copy of this builder
     */
    public FunctionScoreBuilder copy() {
        FunctionScoreBuilder copy = new FunctionScoreBuilder();
        copy.parameters.putAll(this.parameters);
        copy.functions = this.functions.stream().map(Elastic::copyJSON).collect(Collectors.toList());
        return copy;
    }
}
