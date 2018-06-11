/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.query;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class BoolQueryBuilder extends BaseFilter {

    private List<JSONObject> must;
    private List<JSONObject> mustNot;
    private List<JSONObject> should;
    private List<JSONObject> filter;

    private <X> List<X> autoinit(List<X> list) {
        if (list == null) {
            return new ArrayList<>();
        }

        return list;
    }

    public BoolQueryBuilder must(JSONObject filter) {
        if (filter != null) {
            this.must = autoinit(this.must);
            this.must.add(filter);
        }

        return this;
    }

    public BoolQueryBuilder must(Filter filter) {
        return must(filter.toJSON());
    }

    public BoolQueryBuilder mustNot(JSONObject filter) {
        if (filter != null) {
            this.mustNot = autoinit(this.mustNot);
            this.mustNot.add(filter);
        }

        return this;
    }

    public BoolQueryBuilder mustNot(Filter filter) {
        return mustNot(filter.toJSON());
    }

    public BoolQueryBuilder should(JSONObject filter) {
        if (filter != null) {
            this.should = autoinit(this.should);
            this.should.add(filter);
        }

        return this;
    }

    public BoolQueryBuilder should(Filter filter) {
        return should(filter.toJSON());
    }

    public BoolQueryBuilder filter(JSONObject filter) {
        if (filter != null) {
            this.filter = autoinit(this.filter);
            this.filter.add(filter);
        }

        return this;
    }

    public BoolQueryBuilder filter(Filter filter) {
        return filter(filter.toJSON());
    }

    @Override
    public JSONObject toJSON() {
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
            query.put("mustNot", mustNot);
        }
        if (shoulds > 0) {
            query.put("should", should);
        }
        if (filters > 0) {
            query.put("filter", filter);
        }

        return new JSONObject().fluentPut("bool", query);
    }
}
