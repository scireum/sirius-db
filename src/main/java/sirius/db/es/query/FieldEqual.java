/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.query;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticEntity;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * Represents a constraint which checks if the given field has the given value.
 */
public class FieldEqual extends BaseFilter {
    private final String field;
    private Object value;
    private boolean ignoreNull = false;
    private Float boost = null;

    /*
     * Use the #on(String, Object) factory method
     */
    public FieldEqual(String field, Object value) {
        // In search queries the id field must be referenced via "_id" not "id..
        if (ElasticEntity.ID.getName().equalsIgnoreCase(field)) {
            this.field = Elastic.ID_FIELD;
        } else {
            this.field = field;
        }
        this.value = transformFilterValue(value);
    }

    public FieldEqual(Mapping key, Object value) {
        this(key.toString(), value);
    }

    /**
     * Converts the given value into an effective value used to filter in ES.
     * <p>
     * For example an entity will be converted into its ID or an Enum into its name.
     *
     * @param value the value to convert.
     * @return the converted value. If there is no conversion appropriate, the original value will be returned
     */
    public static Object transformFilterValue(Object value) {
        if (value != null && value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof ElasticEntity) {
            return ((ElasticEntity) value).getId();
        }
        //TODO
//        if (value instanceof EntityRef) {
//            return ((EntityRef<?>) value).getId();
//        }
        if (value instanceof Value) {
            return ((Value) value).asString();
        }
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

    /**
     * Makes the filter ignore <tt>null</tt> values (no constraint will be created).
     *
     * @return the constraint itself for fluent method calls
     */
    public FieldEqual ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    /**
     * Sets the boost value that should be used for matching terms.
     *
     * @param boost the boost value
     * @return the constraint itself for fluent method calls
     */
    public FieldEqual withBoost(Float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public JSONObject toJSON() {
        if (Strings.isEmpty(value)) {
            if (ignoreNull) {
                return null;
            }

            return new NotFilled(field).withBoost(boost).toJSON();
        }

        if (boost == null) {
            return new JSONObject().fluentPut("term", new JSONObject().fluentPut(field, value));
        } else {
            return new JSONObject().fluentPut("term",
                                              new JSONObject().fluentPut(field,
                                                                         new JSONObject().fluentPut("value", value)
                                                                                         .fluentPut("boost", boost)));
        }
    }
}
