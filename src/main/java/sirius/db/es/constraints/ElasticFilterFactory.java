/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.constraints;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.Elastic;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.properties.StringMapProperty;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.query.constraints.CSVFilter;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;

import javax.annotation.Nonnull;
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
    private static final String PARAM_DIS_MAX = "dis_max";
    private static final String PARAM_QUERIES = "queries";

    @Override
    protected Object customTransform(Object value) {
        if (value instanceof Instant instant) {
            value = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        }
        if (value instanceof TemporalAccessor temporalAccessor) {
            if (temporalAccessor.isSupported(ChronoField.HOUR_OF_DAY)) {
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(temporalAccessor);
            } else {
                return DateTimeFormatter.ISO_LOCAL_DATE.format(temporalAccessor);
            }
        }

        return value;
    }

    private ElasticConstraint wrap(ObjectNode jsonObject) {
        if (jsonObject == null) {
            return null;
        }

        return new ElasticConstraint(jsonObject);
    }

    @Override
    protected ElasticConstraint eqValue(Mapping field, Object value) {
        return wrap(Json.createObject().set("term", Json.createObject().putPOJO(field.toString(), value)));
    }

    @Override
    protected ElasticConstraint neValue(Mapping field, Object value) {
        return not(eq(field, value));
    }

    private ElasticConstraint rangeFilter(Mapping field, String bound, Object value) {
        return wrap(Json.createObject()
                        .set("range",
                             Json.createObject()
                                 .set(field.toString(), Json.createObject().putPOJO(bound, transform(value)))));
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
        return wrap(Json.createObject().set("exists", Json.createObject().put("field", field.toString())));
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

    @Override
    protected ElasticConstraint invert(ElasticConstraint constraint) {
        return new ElasticConstraint(new BoolQueryBuilder().mustNot(constraint).build());
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
     * See: <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html">Nested Query Documentation</a>
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

        ObjectNode settings = Json.createObject().put(PARAM_VALUE, value).put(PARAM_REWRITE, TOP_TERMS_256);
        return wrap(Json.createObject().set(PARAM_PREFIX, Json.createObject().set(field.toString(), settings)));
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

        ObjectNode settings = Json.createObject().put(PARAM_QUERY, value);
        return wrap(Json.createObject().set(PARAM_MATCH_PHRASE, Json.createObject().set(field.toString(), settings)));
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

        ObjectNode settings = Json.createObject().put(PARAM_VALUE, value);
        return wrap(Json.createObject().set(PARAM_REGEXP, Json.createObject().set(field.toString(), settings)));
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

        ObjectNode settings = Json.createObject().put(PARAM_VALUE, value);
        return wrap(Json.createObject().set(PARAM_WILDCARD, Json.createObject().set(field.toString(), settings)));
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

        ObjectNode settings = Json.createObject()
                                  .put(PARAM_VALUE, value)
                                  .put(PARAM_FUZZINESS, fuzziness == null ? FUZZINESS_AUTO : fuzziness)
                                  .put(PARAM_MAX_EXPANSIONS, maxExpansions)
                                  .put(PARAM_PREFIX_LENGTH, prefixLength)
                                  .put(PARAM_TRANSPOSITIONS, transpositions)
                                  .put(PARAM_REWRITE, rewrite == null ? CONSTANT_SCORE : rewrite);
        return new ElasticConstraint(Json.createObject()
                                         .set(PARAM_FUZZY, Json.createObject().set(field.toString(), settings)));
    }

    /**
     * Creates a query that matches everything.
     *
     * @return a new match_all query.
     */
    public ElasticConstraint matchAll() {
        return wrap(Json.createObject().set(PARAM_MATCH_ALL, Json.createObject()));
    }

    /**
     * Creates a query that matches nothing.
     *
     * @return a new match_none query.
     */
    public ElasticConstraint matchNone() {
        return wrap(Json.createObject().set(PARAM_MATCH_NONE, Json.createObject()));
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
        ObjectNode jsonObject = Json.createObject();

        jsonObject.set(CONSTANT_SCORE,
                       Json.createObject().put(PARAM_BOOST, boost).set(PARAM_FILTER, constraint.toJSON()));

        return new ElasticConstraint(jsonObject);
    }

    /**
     * Creates a "dis_max" query.
     * <p>
     * This is mostly equivalent to {@link #or(List)}. However, instead of summing the scores of each child clause,
     * the best (max) is picked.
     *
     * @param constraints the constraint clauses to add (at least one has to match in order for the query to match)
     * @return the newly created constraint or <tt>null</tt> if the list was empty or did only contain <tt>null</tt>
     * constraints
     */
    @Nullable
    public ElasticConstraint maxScore(@Nonnull List<ElasticConstraint> constraints) {
        List<ElasticConstraint> effectiveConstraints = constraints.stream().filter(Objects::nonNull).toList();
        if (effectiveConstraints.isEmpty()) {
            return null;
        }

        if (effectiveConstraints.size() == 1) {
            return effectiveConstraints.get(0);
        }

        ArrayNode queries = Json.createArray();
        effectiveConstraints.forEach(constraint -> queries.add(constraint.toJSON()));

        return new ElasticConstraint(Json.createObject()
                                         .set(PARAM_DIS_MAX, Json.createObject().set(PARAM_QUERIES, queries)));
    }

    /**
     * Provides a var-args version of {@link #maxScore(List)}.
     * 
     * @param constraints the clauses to match
     * @return the resulting constraint
     * @see #maxScore(List) 
     */
    @Nullable
    public ElasticConstraint maxScore(@Nonnull ElasticConstraint... constraints) {
        return maxScore(Arrays.asList(constraints));
    }
}
