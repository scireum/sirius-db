/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.constraints;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.properties.StringMapProperty;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.query.constraints.CSVFilter;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Generates filters and constraints for {@link sirius.db.es.ElasticQuery}.
 *
 * @see Elastic#FILTERS
 */
public class ElasticFilterFactory extends FilterFactory<ElasticConstraint> {

    private static final String PARAM_VALUE = "value";
    private static final String PARAM_REWRITE = "rewrite";
    private static final String TOP_TERMS_256 = "top_terms_256";
    private static final String PARAM_PREFIX = "prefix";
    private static final String PARAM_QUERY = "query";
    private static final String PARAM_MATCH_PHRASE = "match_phrase";
    private static final String PARAM_REGEXP = "regexp";
    private static final String PARAM_WILDCARD = "wildcard";
    private static final String PARAM_FUZZINESS = "fuzziness";
    private static final String FUZZINESS_AUTO = "auto";
    private static final String PARAM_MAX_EXPANSIONS = "max_expansions";
    private static final String PARAM_PREFIX_LENGTH = "prefix_length";
    private static final String PARAM_TRANSPOSITIONS = "transpositions";
    private static final String CONSTANT_SCORE = "constant_score";
    private static final String PARAM_FUZZY = "fuzzy";
    private static final String PARAM_MATCH_ALL = "match_all";
    private static final String PARAM_MATCH_NONE = "match_none";
    private static final String PARAM_FILTER = "filter";
    private static final String PARAM_BOOST = "boost";

    @Override
    protected Object customTransform(Object value) {
        if (value instanceof Instant) {
            value = LocalDateTime.ofInstant((Instant) value, ZoneId.systemDefault());
        }
        if (value instanceof TemporalAccessor) {
            if (((TemporalAccessor) value).isSupported(ChronoField.HOUR_OF_DAY)) {
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format((TemporalAccessor) value);
            } else {
                return DateTimeFormatter.ISO_LOCAL_DATE.format((TemporalAccessor) value);
            }
        }

        return value;
    }

    private ElasticConstraint wrap(JSONObject jsonObject) {
        if (jsonObject == null) {
            return null;
        }

        return new ElasticConstraint(jsonObject);
    }

    private String determineFilterField(Mapping field) {
        return ElasticEntity.ID.equals(field) ? Elastic.ID_FIELD : field.toString();
    }

    @Override
    protected ElasticConstraint eqValue(Mapping field, Object value) {
        return wrap(new JSONObject().fluentPut("term", new JSONObject().fluentPut(determineFilterField(field), value)));
    }

    @Override
    protected ElasticConstraint neValue(Mapping field, Object value) {
        return not(eq(field, value));
    }

    private ElasticConstraint rangeFilter(Mapping field, String bound, Object value) {
        return wrap(new JSONObject().fluentPut("range",
                                               new JSONObject().fluentPut(determineFilterField(field),
                                                                          new JSONObject().fluentPut(bound,
                                                                                                     transform(value)))));
    }

    @Override
    protected ElasticConstraint gtValue(Mapping field, Object value, boolean orEqual) {
        return rangeFilter(field, orEqual ? "gte" : "gt", value);
    }

    @Override
    protected ElasticConstraint ltValue(Mapping field, Object value, boolean orEqual) {
        return rangeFilter(field, orEqual ? "lte" : "lt", value);
    }

    /**
     * Checks whether the given field is filled which means whether the given field is present, as elastic doesn't index
     * null-values by default. The field is just not created.
     *
     * @param field the field to check
     * @return the generated constraint
     */
    @Override
    public ElasticConstraint filled(Mapping field) {
        return wrap(new JSONObject().fluentPut("exists",
                                               new JSONObject().fluentPut("field", determineFilterField(field))));
    }

    /**
     * Checks whether the given field is not filled which means whether the given field is absent, as elastic doesn't index
     * null-values by default. The field is just not created.
     *
     * @param field the field to check
     * @return the generated constraint
     */
    @Override
    public ElasticConstraint notFilled(Mapping field) {
        return not(filled(field));
    }

    /**
     * As elastic doesn't index null-values by default an exists query is basically the same as a {@link #filled(Mapping)} query.
     *
     * @param field the field to check
     * @return the generated constraint
     */
    public ElasticConstraint exists(Mapping field) {
        return filled(field);
    }

    /**
     * As elastic doesn't index null-values by default a notExists query is basically the same as a {@link #notFilled(Mapping)} query.
     *
     * @param field the field to check
     * @return the generated constraint
     */
    public ElasticConstraint notExists(Mapping field) {
        return notFilled(field);
    }

    @Override
    protected ElasticConstraint invert(ElasticConstraint constraint) {
        return wrap(new BoolQueryBuilder().mustNot(constraint).build());
    }

    @Override
    protected ElasticConstraint effectiveAnd(List<ElasticConstraint> effectiveConstraints) {
        BoolQueryBuilder qry = new BoolQueryBuilder();
        effectiveConstraints.forEach(qry::must);
        return wrap(qry.build());
    }

    @Override
    protected ElasticConstraint effectiveOr(List<ElasticConstraint> effectiveConstraints) {
        BoolQueryBuilder qry = new BoolQueryBuilder();
        effectiveConstraints.forEach(qry::should);
        return wrap(qry.build());
    }

    /**
     * Generates an AND constraint just like {@link #and(List)} but gives the generated query a name.
     * <p>
     * This can later be checked using {@link sirius.db.es.ElasticEntity#isMatchedNamedQuery(String)}.
     *
     * @param name        the name of the query to use
     * @param constraints the constraints to combine
     * @return the newly generated named query
     */
    public ElasticConstraint namedAnd(String name, List<ElasticConstraint> constraints) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        constraints.stream().filter(Objects::nonNull).forEach(query::must);
        query.named(name);
        return wrap(query.build());
    }

    /**
     * Provides a boilerplate for {@link #namedAnd(String, List)} which accepts constraints as varargs.
     *
     * @param name        the name of the query to use
     * @param constraints the constraints to combine
     * @return the newly generated named query
     */
    public ElasticConstraint namedAnd(String name, ElasticConstraint... constraints) {
        return namedAnd(name, Arrays.asList(constraints));
    }

    /**
     * Generates an OR constraint just like {@link #or(List)} but gives the generated query a name.
     * <p>
     * This can later be checked using {@link sirius.db.es.ElasticEntity#isMatchedNamedQuery(String)}.
     *
     * @param name        the name of the query to use
     * @param constraints the constraints to combine
     * @return the newly generated named query
     */
    public ElasticConstraint namedOr(String name, List<ElasticConstraint> constraints) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        constraints.stream().filter(Objects::nonNull).forEach(query::should);
        query.named(name);
        return wrap(query.build());
    }

    /**
     * Provides a boilerplate for {@link #namedOr(String, List)} which accepts constraints as varargs.
     *
     * @param name        the name of the query to use
     * @param constraints the constraints to combine
     * @return the newly generated named query
     */
    public ElasticConstraint namedOr(String name, ElasticConstraint... constraints) {
        return namedOr(name, Arrays.asList(constraints));
    }

    @Override
    public CSVFilter<ElasticConstraint> containsAny(Mapping field, Value commaSeparatedValues) {
        return new ElasticCSVFilter(this, field, commaSeparatedValues.asString(), CSVFilter.Mode.CONTAINS_ANY);
    }

    @Override
    public CSVFilter<ElasticConstraint> containsAll(Mapping field, Value commaSeparatedValues) {
        return new ElasticCSVFilter(this, field, commaSeparatedValues.asString(), CSVFilter.Mode.CONTAINS_ALL);
    }

    @Override
    public Tuple<ElasticConstraint, Boolean> compileString(EntityDescriptor descriptor,
                                                           String query,
                                                           List<QueryField> fields) {
        ElasticQueryCompiler compiler = new ElasticQueryCompiler(this, descriptor, query, fields);
        return Tuple.create(compiler.compile(), compiler.isDebugging());
    }

    /**
     * Executes a <tt>nested</tt> query.
     * <p>
     * Nested queries must be used along with {@link sirius.db.mixing.types.NestedList nested lists}. Otherwise,
     * if there are several objects nested in a list, an entity would also match, if the filtered properties match
     * against any of the nested objects instead of all in a single one - which is most commonly wanted.
     * <p>
     * See: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html
     *
     * @param field the list or inner map to query
     * @return a nested query which can be populated with constraints
     */
    public NestedQuery nested(Mapping field) {
        return new NestedQuery(field);
    }

    /**
     * Creates a filter which ensures, that a nested {@link sirius.db.mixing.types.StringMap}
     * or {@link sirius.db.mixing.types.StringListMap} contains the given key and value.
     *
     * @param mapField the property which contains either a <tt>StringMap</tt> or <tt>StringListMap</tt>
     * @param key      the key to check for
     * @param value    the value to check for
     * @return the filter
     */
    public ElasticConstraint nestedMapContains(Mapping mapField, String key, String value) {
        return nested(mapField).eq(mapField.nested(Mapping.named(StringMapProperty.KEY)), key)
                               .eq(mapField.nested(Mapping.named(StringMapProperty.VALUE)), value)
                               .build();
    }

    /**
     * Creates a prefix query.
     *
     * @param field the field to search in
     * @param value the prefix to filter by
     * @return a new prefix query as constraint
     */
    public ElasticConstraint prefix(Mapping field, String value) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        JSONObject settings = new JSONObject().fluentPut(PARAM_VALUE, value).fluentPut(PARAM_REWRITE, TOP_TERMS_256);
        return wrap(new JSONObject().fluentPut(PARAM_PREFIX,
                                               new JSONObject().fluentPut(determineFilterField(field), settings)));
    }

    /**
     * Creates a <tt>match_phrase</tt> query.
     * <p>
     * In contrast to simply filtering on each term, this also ensures the proper order of the terms.
     *
     * @param field the field to search in
     * @param value the phrase (whitespace separated) to filter by
     * @return a new phrase query as constraint
     */
    public ElasticConstraint phrase(Mapping field, String value) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        JSONObject settings = new JSONObject().fluentPut(PARAM_QUERY, value);
        return wrap(new JSONObject().fluentPut(PARAM_MATCH_PHRASE,
                                               new JSONObject().fluentPut(determineFilterField(field), settings)));
    }

    /**
     * Creates a <tt>regular expression</tt> query.
     *
     * @param field the field to search in
     * @param value the regular expression to filter on
     * @return a new regex query as constraint
     */
    public ElasticConstraint regexp(Mapping field, String value) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        JSONObject settings = new JSONObject().fluentPut(PARAM_VALUE, value);
        return wrap(new JSONObject().fluentPut(PARAM_REGEXP,
                                               new JSONObject().fluentPut(determineFilterField(field), settings)));
    }

    /**
     * Creates a <tt>wildcard</tt> query.
     *
     * @param field the field to search in
     * @param value the regular expression to filter on
     * @return a new wildcard query as constraint
     */
    public ElasticConstraint wildcard(Mapping field, String value) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        JSONObject settings = new JSONObject().fluentPut(PARAM_VALUE, value);
        return wrap(new JSONObject().fluentPut(PARAM_WILDCARD,
                                               new JSONObject().fluentPut(determineFilterField(field), settings)));
    }

    /**
     * Generates a fuzzy search constraint.
     *
     * @param field          the field to search in
     * @param value          the value to search for
     * @param fuzziness      defines the levenstein edit distance which is permitted or "AUTO" or <tt>null</tt> to let
     *                       Elasticsearch determine an appropriate distance based on the given token length
     * @param maxExpansions  defines the max expansions to use.
     *                       The default value is 50.
     * @param prefixLength   defines the prefix length for which no edits are attempted.
     *                       The default is 0.
     * @param transpositions determines if transpositions (ab -> ba) are considered a single edit.
     *                       The default is <tt>true</tt>.
     * @param rewrite        determines which rewrite is applied. Use "constant_score" or <tt>null</tt> as default
     * @return the generated constraint
     */
    public ElasticConstraint fuzzy(Mapping field,
                                   String value,
                                   @Nullable String fuzziness,
                                   int maxExpansions,
                                   int prefixLength,
                                   boolean transpositions,
                                   @Nullable String rewrite) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        JSONObject settings = new JSONObject().fluentPut(PARAM_VALUE, value)
                                              .fluentPut(PARAM_FUZZINESS,
                                                         fuzziness == null ? FUZZINESS_AUTO : fuzziness)
                                              .fluentPut(PARAM_MAX_EXPANSIONS, maxExpansions)
                                              .fluentPut(PARAM_PREFIX_LENGTH, prefixLength)
                                              .fluentPut(PARAM_TRANSPOSITIONS, transpositions)
                                              .fluentPut(PARAM_REWRITE, rewrite == null ? CONSTANT_SCORE : rewrite);
        return new ElasticConstraint(new JSONObject().fluentPut(PARAM_FUZZY,
                                                                new JSONObject().fluentPut(determineFilterField(field),
                                                                                           settings)));
    }

    /**
     * Creates a query that matches everything.
     *
     * @return a new match_all query.
     */
    public ElasticConstraint matchAll() {
        return wrap(new JSONObject().fluentPut(PARAM_MATCH_ALL, new JSONObject()));
    }

    /**
     * Creates a query that matches nothing.
     *
     * @return a new match_none query.
     */
    public ElasticConstraint matchNone() {
        return wrap(new JSONObject().fluentPut(PARAM_MATCH_NONE, new JSONObject()));
    }

    /**
     * Creates a new constraint with a constant score.
     * <p>
     * Note that this must be applied using {@link sirius.db.es.ElasticQuery#must(ElasticConstraint)}, as otherwise,
     * all scoring is suppressed via a <tt>BooleanTermQuery.filter</tt> (when using <tt>ElasticQuery.where</tt>).
     *
     * @param constraint the constraint to be fullfilled
     * @param boost      the boost value to attach to an entity matching the given constraint
     * @return a new constraint which represents the given one along with the boost to apply
     * @see ElasticConstraint#withConstantScore(float)
     */
    public ElasticConstraint constantScore(ElasticConstraint constraint, float boost) {
        JSONObject jsonObject = new JSONObject();

        jsonObject.put(CONSTANT_SCORE,
                       new JSONObject().fluentPut(PARAM_FILTER, constraint.toJSON()).fluentPut(PARAM_BOOST, boost));

        return new ElasticConstraint(jsonObject);
    }
}
