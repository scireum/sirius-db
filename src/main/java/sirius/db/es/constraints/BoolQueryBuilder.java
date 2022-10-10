/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.constraints;

import com.alibaba.fastjson2.JSONObject;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticQuery;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Represents a boolean query which is the actual work horse of Elasticsearch queries.
 */
public class BoolQueryBuilder {

    private List<JSONObject> must;
    private List<JSONObject> mustNot;
    private List<JSONObject> should;
    private List<JSONObject> filter;
    private String name;

    private List<JSONObject> autoinit(List<JSONObject> list) {
        if (list == null) {
            return new ArrayList<>();
        }

        return list;
    }

    /**
     * Adds a MUST constraint for the given query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder must(JSONObject filter) {
        if (filter != null) {
            this.must = autoinit(this.must);
            this.must.add(filter);
        }

        return this;
    }

    /**
     * Adds a MUST constraint for the given query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder must(ElasticConstraint filter) {
        return must(filter.toJSON());
    }

    /**
     * Adds a MUST NOT constraint for the given query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder mustNot(JSONObject filter) {
        if (filter != null) {
            this.mustNot = autoinit(this.mustNot);
            this.mustNot.add(filter);
        }

        return this;
    }

    /**
     * Adds a MUST NOT constraint for the given query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder mustNot(ElasticConstraint filter) {
        return mustNot(filter.toJSON());
    }

    /**
     * Adds a SHOULD constraint for the given query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder should(JSONObject filter) {
        if (filter != null) {
            this.should = autoinit(this.should);
            this.should.add(filter);
        }

        return this;
    }

    /**
     * Adds a SHOULD constraint for the given query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder should(ElasticConstraint filter) {
        return should(filter.toJSON());
    }

    /**
     * Adds a FILTER constraint for the given query.
     * <p>
     * A filter is like a MUST clause but without scoring.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder filter(JSONObject filter) {
        if (filter != null) {
            this.filter = autoinit(this.filter);
            this.filter.add(filter);
        }

        return this;
    }

    /**
     * Adds a FILTER constraint for the given query.
     * <p>
     * A filter is like a MUST clause but without scoring.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder filter(ElasticConstraint filter) {
        return filter(filter.toJSON());
    }

    /**
     * Removes all filters ({@link #filter(JSONObject)}) for which the given predicate matches.
     *
     * @param shouldRemove the predicate to determine which filters to remove
     */
    public void removeFilterIf(Predicate<JSONObject> shouldRemove) {
        if (filter != null) {
            filter.removeIf(shouldRemove);
        }
    }

    /**
     * Specifies the query name to use.
     * <p>
     * This can later be checked using {@link sirius.db.es.ElasticEntity#isMatchedNamedQuery(String)}.
     *
     * @param name the name of the query
     * @return the query itself for fluent method calls
     */
    public BoolQueryBuilder named(String name) {
        this.name = name;
        return this;
    }

    /**
     * Compiles the boolen query into a constraint.
     *
     * @return the query as constraint
     */
    @SuppressWarnings("squid:MethodCyclomaticComplexity")
    @Explain("Splitting this method would most probably increase the complexity.")
    @Nullable
    public JSONObject build() {
        int filters = filter == null ? 0 : filter.size();
        int musts = must == null ? 0 : must.size();
        int mustNots = mustNot == null ? 0 : mustNot.size();
        int shoulds = should == null ? 0 : should.size();

        if (filters == 0 && musts == 0 && mustNots == 0 && shoulds == 0) {
            return null;
        }

        if (musts == 1 && mustNots == 0 && filters == 0 && shoulds == 0) {
            return must.get(0);
        }

        JSONObject query = new JSONObject();
        if (musts > 0) {
            query.put("must", must);
        }
        if (mustNots > 0) {
            query.put("must_not", mustNot);
        }
        if (shoulds > 0) {
            query.put("should", should);
        }
        if (filters > 0) {
            query.put("filter", filter);
        }

        JSONObject result = new JSONObject().fluentPut("bool", query);
        if (Strings.isFilled(name)) {
            query.put("_name", name);
        }
        return result;
    }

    /**
     * Generates a copy of this query builder to support {@link ElasticQuery#copy()}.
     *
     * @return a copy of this builder
     */
    public BoolQueryBuilder copy() {
        BoolQueryBuilder copy = new BoolQueryBuilder();
        if (must != null) {
            copy.must = this.must.stream().map(Elastic::copyJSON).collect(Collectors.toList());
        }
        if (mustNot != null) {
            copy.mustNot = this.mustNot.stream().map(Elastic::copyJSON).collect(Collectors.toList());
        }
        if (should != null) {
            copy.should = this.should.stream().map(Elastic::copyJSON).collect(Collectors.toList());
        }
        if (filter != null) {
            copy.filter = this.filter.stream().map(Elastic::copyJSON).collect(Collectors.toList());
        }

        copy.name = name;

        return copy;
    }
}
