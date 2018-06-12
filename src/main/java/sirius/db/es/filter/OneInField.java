/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.filter;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticEntity;
import sirius.db.mixing.Mapping;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a constraint which verifies that the given field contains one of the given values.
 */
public class OneInField extends BaseFilter {

    private final Collection<?> values;
    private final String field;
    private boolean orEmpty = false;
    private boolean forceEmpty = false;

    /**
     * Creates a filter which ensures that none of the given values is in the given field.
     *
     * @param field  the field to filter on
     * @param values the values to exclude
     */
    public OneInField(Mapping field, Collection<?> values) {
        this(values, field.toString());
    }

    /**
     * Creates a filter which ensures that at least one of the given values is in the given field.
     *
     * @param field  the field to filter on
     * @param values the values to exclude
     */
    public OneInField(Collection<?> values, String field) {
        if (values != null) {
            this.values = values.stream().filter(Objects::nonNull).collect(Collectors.<Object>toList());
        } else {
            this.values = null;
        }
        // In search queries the id field must be referenced via "_id" not "id..
        if (ElasticEntity.ID.getName().equalsIgnoreCase(field)) {
            this.field = Elastic.ID_FIELD;
        } else {
            this.field = field;
        }
    }

    /**
     * Signals that this constraint is also fulfilled if the target field is empty.
     * <p>
     * This will convert this constraint into a filter.
     *
     * @return the constraint itself for fluent method calls
     */
    public OneInField orEmpty() {
        orEmpty = true;
        return this;
    }

    /**
     * Signals that an empty input list is not ignored but enforces the target field to be empty.
     * <p>
     * This will convert this constraint into a filter.
     *
     * @return the constraint itself for fluent method calls
     */
    public OneInField forceEmpty() {
        orEmpty = true;
        forceEmpty = true;
        return this;
    }

    @Override
    public JSONObject toJSON() {
        if (values == null || values.isEmpty()) {
            if (forceEmpty) {
                return new NotFilled(field).toJSON();
            }

            return null;
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        for (Object value : values) {
            boolQueryBuilder.should(new FieldEqual(field, value));
        }
        if (orEmpty) {
            boolQueryBuilder.should(new NotFilled(field));
        }

        return boolQueryBuilder.toJSON();
    }
}
