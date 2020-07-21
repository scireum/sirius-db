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
import sirius.db.mixing.Mapping;
import sirius.db.mixing.query.constraints.CSVFilter;
import sirius.db.mixing.query.constraints.FilterFactory;

import java.util.List;

/**
 * Generates a optimized constraint for ES.
 */
public class ElasticCSVFilter extends CSVFilter<ElasticConstraint> {

    protected ElasticCSVFilter(FilterFactory<ElasticConstraint> factory, Mapping field, String value, Mode mode) {
        super(factory, field, value, mode);
    }

    @Override
    public ElasticConstraint build() {
        List<String> values = collectValues();
        if (values.isEmpty()) {
            return null;
        }

        BoolQueryBuilder bqb = new BoolQueryBuilder();
        if (mode == Mode.CONTAINS_ANY) {
            for (String val : values) {
                bqb.should(Elastic.FILTERS.eq(field, val));
            }
        } else if (mode == Mode.CONTAINS_ALL) {
            for (String val : values) {
                bqb.must(Elastic.FILTERS.eq(field, val));
            }
        }
        if (orEmpty) {
            bqb.should(Elastic.FILTERS.notFilled(field));
        }

        JSONObject result = bqb.build();
        if (result == null) {
            return null;
        }

        return new ElasticConstraint(result);
    }
}
