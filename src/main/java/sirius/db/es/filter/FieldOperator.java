/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.filter;

import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.Mapping;

/**
 * Represents a relational filter which can be used to filter &lt; or &lt;=, along with &gt; or &gt;=
 */
public class FieldOperator extends BaseFilter {

    private enum Bound {
        LT("lt"), LT_EQ("lte"), GT("gt"), GT_EQ("gte");

        private String value;

        Bound(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private String field;
    private Object value;
    private Bound bound;
    private boolean orEmpty = false;

    /*
     * Use one of the factory methods
     */
    private FieldOperator(String field) {
        this.field = field;
    }

    /**
     * Creates a new constraint representing <tt>field &lt; value</tt>
     *
     * @param field the field to check
     * @param value the value to compare to
     * @return the newly constructed constraint
     */
    public static FieldOperator less(Mapping field, Object value) {
        return less(field.toString(), value);
    }

    /**
     * Creates a new constraint representing <tt>field &lt; value</tt>
     *
     * @param field the field to check
     * @param value the value to compare to
     * @return the newly constructed constraint
     */
    public static FieldOperator less(String field, Object value) {
        FieldOperator result = new FieldOperator(field);
        result.bound = Bound.LT;
        result.value = FieldEqual.transformFilterValue(value);

        return result;
    }

    /**
     * Creates a new constraint representing <tt>field &gt; value</tt>
     *
     * @param field the field to check
     * @param value the value to compare to
     * @return the newly constructed constraint
     */
    public static FieldOperator greater(Mapping field, Object value) {
        return greater(field.toString(), value);
    }

    /**
     * Creates a new constraint representing <tt>field &gt; value</tt>
     *
     * @param field the field to check
     * @param value the value to compare to
     * @return the newly constructed constraint
     */
    public static FieldOperator greater(String field, Object value) {
        FieldOperator result = new FieldOperator(field);
        result.bound = Bound.GT;
        result.value = FieldEqual.transformFilterValue(value);
        return result;
    }

    /**
     * Makes the filter include its limit.
     * <p>
     * Essentially this converts &lt; to &lt;= and &gt; to &gt;=
     *
     * @return the constraint itself for fluent method calls
     */
    public FieldOperator including() {
        if (bound == Bound.LT) {
            bound = Bound.LT_EQ;
        } else if (bound == Bound.GT) {
            bound = Bound.GT_EQ;
        }

        return this;
    }

    /**
     * Signals that this constraint is also fulfilled if the target field is empty.
     * <p>
     * This will convert this constraint into a filter.
     *
     * @return the constraint itself for fluent method calls
     */
    public FieldOperator orEmpty() {
        this.orEmpty = true;
        return this;
    }

    @Override
    public JSONObject toJSON() {
        if (value == null) {
            return null;
        }

        JSONObject query = new JSONObject().fluentPut("range",
                                                      new JSONObject().fluentPut(field,
                                                                                 new JSONObject().fluentPut(bound.toString(),
                                                                                                            value)));
        if (orEmpty) {
            return new BoolQueryBuilder().should(query).should(new NotFilled(field)).toJSON();
        } else {
            return query;
        }
    }
}
