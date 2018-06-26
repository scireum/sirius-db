/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.SmartQuery;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Strings;

import javax.annotation.Nullable;

/**
 * Represents a LIKE constraint.
 * <p>
 * This can be used to generate queries like {@code x LIKE 'a%'}. Using the helper methods this can also be used to
 * search for occurrences of serveral words in several fields, this can be used as a general table search.
 */
public class Like {

    private static final String WILDCARD = "*";
    private Mapping field;
    private String value;
    private boolean ignoreEmpty;
    private boolean ignoreCase;

    protected Like(Mapping field) {
        this.field = field;
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
     * Sepcifies a value with which the target field needs to start.
     * <p>
     * This is roughly the same as calling {@code matches(value+"*")}.
     *
     * @param value the value to search for
     * @return the constaint itself
     */
    public Like startsWith(String value) {
        String effectiveValue = value;
        if (Strings.isFilled(effectiveValue)) {
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

    /**
     * Generates a constraint for the given settings
     *
     * @return a constraint based on the given settings or <tt>null</tt> if no constraint was generated
     */
    @Nullable
    public SQLConstraint build() {
        if (Strings.isEmpty(value) && ignoreEmpty) {
            return null;
        }

        return new LikeConstraint();
    }

    private class LikeConstraint extends SQLConstraint {

        @Override
        public void asString(StringBuilder builder) {
            String effectiveValue = value.replace('*', '%');
            if (ignoreCase) {
                builder.append("LOWER(")
                       .append(field.toString())
                       .append(") LIKE '")
                       .append(effectiveValue.toLowerCase())
                       .append("'");
            } else {
                builder.append(field.toString()).append(" LIKE '").append(effectiveValue.toLowerCase()).append("'");
            }
        }

        @Override
        public void appendSQL(SmartQuery.Compiler compiler) {
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
