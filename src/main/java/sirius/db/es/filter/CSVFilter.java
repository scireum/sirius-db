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
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a constraint which checks if the given field contains one or all of the values given as a comma
 * separated string. By default the given String will be split on "," and "|" but you can also use {@link
 * #customSplitter(String)} to split by a custom Regular Expression.
 * <p>
 * Therefore the constraint translates x,y,z for field f to: {@code f = x OR f = y OR f = z}. Empty strings
 * are gracefully handled (ignored). If {@link #orEmpty()} is used, the constraint also succeeds if
 * the target field is empty. This is only valid when <tt>containsAny</tt> is used.
 */
public class CSVFilter extends BaseFilter {

    private static final String DEFAULT_SPLITTER = "[,|]";

    /**
     * Specifies the matching mode for a filter.
     */
    enum Mode {
        CONTAINS_ANY, CONTAINS_ALL
    }

    private final String field;
    private List<String> values;
    private boolean orEmpty = false;
    private Mode mode;
    private String commaSeparatedValues;
    private String splitter;
    private boolean lowercaseValues;
    private boolean uppercaseValues;

    /*
     * Use one of the factory methods
     */
    private CSVFilter(String field, String value, Mode mode) {
        // In search queries the id field must be referenced via "_id" not "id..
        if (ElasticEntity.ID.getName().equalsIgnoreCase(field)) {
            this.field = Elastic.ID_FIELD;
        } else {
            this.field = field;
        }
        this.splitter = DEFAULT_SPLITTER;
        this.mode = mode;
        this.commaSeparatedValues = value;
    }

    /**
     * Creates a new constraint for the given field which asserts that one of the given values in the string is
     * present.
     * <p>
     * The string can have a form like A,B,C or A|B|C.
     *
     * @param field                the field to check
     * @param commaSeparatedValues the comma separated values to check for
     * @return a new constraint representing the given filter setting
     */
    public static CSVFilter containsAny(String field, Value commaSeparatedValues) {
        return new CSVFilter(field, commaSeparatedValues.asString(), Mode.CONTAINS_ANY);
    }

    /**
     * Creates a new constraint for the given field which asserts that all of the given values in the string is
     * present.
     * <p>
     * The string can have a form like A,B,C or A|B|C.
     *
     * @param field                the field to check
     * @param commaSeparatedValues the comma separated values to check for
     * @return a new constraint representing the given filter setting
     */
    public static CSVFilter containsAll(String field, Value commaSeparatedValues) {
        return new CSVFilter(field, commaSeparatedValues.asString(), Mode.CONTAINS_ALL);
    }

    /**
     * Signals that this constraint should convert the values to lowercase before being applied
     *
     * @return the constraint itself for fluent method calls
     */
    public CSVFilter lowercaseValues() {
        lowercaseValues = true;
        return this;
    }

    /**
     * Signals that this constraint should convert the values to uppercase before being applied
     *
     * @return the constraint itself for fluent method calls
     */
    public CSVFilter uppercaseValues() {
        uppercaseValues = true;
        return this;
    }

    /**
     * Signals that this constraint should split the give values String using the given Regular Expression
     *
     * @param customSplitter a Regular Expression used to split
     * @return the constraint itself for fluent method calls
     */
    public CSVFilter customSplitter(String customSplitter) {
        splitter = customSplitter;
        return this;
    }

    /**
     * Signals the constraint to also accept an empty target field if <tt>containsAny</tt> is used.
     *
     * @return the constraint itself for fluent method calls
     */
    public CSVFilter orEmpty() {
        if (mode == Mode.CONTAINS_ALL) {
            throw new IllegalStateException("Cannot apply a CONTAINS ALL constraint which accepts empty values.");
        }
        this.orEmpty = true;
        return this;
    }

    @Override
    public JSONObject toJSON() {
        collectValues();
        if (values.isEmpty()) {
            return null;
        }

        BoolQueryBuilder bqb = new BoolQueryBuilder();
        if (mode == Mode.CONTAINS_ANY) {
            for (String val : values) {
                bqb.should(new FieldEqual(field, val));
            }
        } else if (mode == Mode.CONTAINS_ALL) {
            for (String val : values) {
                bqb.must(new FieldEqual(field, val));
            }
        }
        if (orEmpty) {
            bqb.should(new NotFilled(field));
        }

        return bqb.toJSON();
    }

    private void collectValues() {
        if (Strings.isFilled(commaSeparatedValues)) {
            Stream<String> stream = Arrays.stream(commaSeparatedValues.split(splitter))
                                          .filter(Objects::nonNull)
                                          .map(String::trim)
                                          .filter(Strings::isFilled);
            if (lowercaseValues) {
                stream = stream.map(String::toLowerCase);
            }
            if (uppercaseValues) {
                stream = stream.map(String::toUpperCase);
            }
            this.values = stream.collect(Collectors.toList());
        } else {
            this.values = Collections.emptyList();
        }
    }
}
