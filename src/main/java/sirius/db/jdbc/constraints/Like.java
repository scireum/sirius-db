/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import com.google.common.collect.Lists;
import sirius.db.mixing.Mapping;
import sirius.db.jdbc.Constraint;
import sirius.db.jdbc.SmartQuery;
import sirius.kernel.commons.Strings;

import java.util.List;

/**
 * Represents a LIKE constraint.
 * <p>
 * This can be used to generate queries like {@code x LIKE 'a%'}. Using the helper methods this can also be used to
 * search for occurrences of serveral words in several fields, this can be used as a general table search.
 */
public class Like extends Constraint {

    private static final String WILDCARD = "*";
    private Mapping field;
    private String value;
    private boolean ignoreEmpty;
    private boolean ignoreCase;

    private Like(Mapping field) {
        this.field = field;
    }

    /**
     * Generates a constraint which ensures that each word in the query occurs at least in one of the given fields.
     * <p>
     * Splits the given query at each whitespace and generates a constraint which ensures that each of these words
     * occures in it least on of the given fields.
     *
     * @param query  the query to parse
     * @param fields the fields to search in
     * @return a constraint representing the given query in the given fields
     */
    public static Constraint allWordsInAnyField(String query, Mapping... fields) {
        List<Constraint> wordConstraints = Lists.newArrayList();
        for (String word : query.split("\\s")) {
            List<Constraint> fieldConstraints = Lists.newArrayList();
            for (Mapping field : fields) {
                fieldConstraints.add(Like.on(field).contains(word).ignoreCase().ignoreEmpty());
            }
            wordConstraints.add(Or.of(fieldConstraints));
        }
        return And.of(wordConstraints);
    }

    /**
     * Creates a like constraint for the given field.
     *
     * @param field the field to search in
     * @return a like constraint applied to the given field
     */
    public static Like on(Mapping field) {
        return new Like(field);
    }

    /**
     * Specifies a value to match in the given field.
     * <p>
     * Note that "*" will be repalced by "%" as this is the wildcard used by SQL.
     *
     * @param value the text to search for
     * @return the constraint itself
     */
    public Like matches(String value) {
        this.value = value;
        return this;
    }

    /**
     * Sepcifies a value which needs to occur anywhere in the target field.
     * <p>
     * This is roughly the same as calling {@code matches("*"+value+"*")}.
     *
     * @param value the value to search for
     * @return the constaint itself
     */
    public Like contains(String value) {
        String effectiveValue = value;
        if (Strings.isFilled(effectiveValue)) {
            if (!effectiveValue.startsWith(WILDCARD)) {
                effectiveValue = WILDCARD + effectiveValue;
            }
            if (!effectiveValue.endsWith(WILDCARD)) {
                effectiveValue = effectiveValue + WILDCARD;
            }
        }
        this.value = effectiveValue;
        return this;
    }

    /**
     * Specifies that upper- and lowercase should not be distinguished.
     *
     * @return the constraint itself
     */
    public Like ignoreCase() {
        this.ignoreCase = true;
        return this;
    }

    /**
     * Permits to skip this constraint if the given filter value is empty.
     *
     * @return the constraint itself
     */
    public Like ignoreEmpty() {
        this.ignoreEmpty = true;
        return this;
    }

    @Override
    public boolean addsConstraint() {
        return !ignoreEmpty || Strings.isFilled(value);
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        if (addsConstraint()) {
            String effectiveValue = value.replace('*', '%');
            if (ignoreCase) {
                compiler.getWHEREBuilder()
                        .append("LOWER(")
                        .append(compiler.translateColumnName(field))
                        .append(") LIKE ?");
                compiler.addParameter(effectiveValue.toLowerCase());
            } else {
                compiler.getWHEREBuilder().append(compiler.translateColumnName(field)).append(" LIKE ?");
                compiler.addParameter(effectiveValue);
            }
        }
    }
}
