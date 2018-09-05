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
import sirius.kernel.commons.Value;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;

/**
 * Generates filters and constraints for {@link sirius.db.es.ElasticQuery}.
 *
 * @see Elastic#FILTERS
 */
public class ElasticFilterFactory extends FilterFactory<ElasticConstraint> {

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

    @Override
    public ElasticConstraint filled(Mapping field) {
        return effectiveAnd(Arrays.asList(wrap(new JSONObject().fluentPut("exists",
                                                                         new JSONObject().fluentPut("field",
                                                                                                    determineFilterField(
                                                                                                            field)))),
                                         neValue(field, null)));
    }

    @Override
    public ElasticConstraint notFilled(Mapping field) {
        return not(filled(field));
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

    @Override
    public ElasticCSVFilter containsAny(Mapping field, Value commaSeparatedValues) {
        return new ElasticCSVFilter(this, field, commaSeparatedValues.asString(), CSVFilter.Mode.CONTAINS_ANY);
    }

    @Override
    public ElasticCSVFilter containsAll(Mapping field, Value commaSeparatedValues) {
        return new ElasticCSVFilter(this, field, commaSeparatedValues.asString(), CSVFilter.Mode.CONTAINS_ALL);
    }

    @Override
    public ElasticConstraint queryString(EntityDescriptor descriptor, String query, List<QueryField> fields) {
        return new ElasticQueryCompiler(this, descriptor, query, fields).compile();
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

        return wrap(new JSONObject().fluentPut("prefix",
                                               new JSONObject().fluentPut(determineFilterField(field),
                                                                          new JSONObject().fluentPut("value", value)
                                                                                          .fluentPut("rewrite",
                                                                                                     "top_terms_256"))));
    }
}
