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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class which generates a function score query for elasticsearch which can be used via
 * {@link ElasticQuery#functionScore(FunctionScoreBuilder)}.
 */
public class FunctionScoreBuilder {

    private static final String FUNCTION_SCORE = "function_score";
    private static final String QUERY = "query";
    private static final String FUNCTIONS = "functions";

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
     * Applies the given query to the function score query and returns the newly created query.
     *
     * @param query the query to apply
     * @return the function score query as {@link JSONObject}
     */
    public JSONObject apply(JSONObject query) {
        return new JSONObject().fluentPut(FUNCTION_SCORE,
                                          new JSONObject().fluentPut(QUERY, query)
                                                          .fluentPut(FUNCTIONS,
                                                                     new JSONArray(new ArrayList<Object>(functions)))
                                                          .fluentPutAll(parameters));
    }

    /**
     * Builds the function score query.
     *
     * @return the function score query as {@link JSONObject}
     */
    public JSONObject build() {
        return new JSONObject().fluentPut(FUNCTION_SCORE,
                                          new JSONObject().fluentPut(FUNCTIONS,
                                                                     new JSONArray(new ArrayList<Object>(functions)))
                                                          .fluentPutAll(parameters));
    }
}
