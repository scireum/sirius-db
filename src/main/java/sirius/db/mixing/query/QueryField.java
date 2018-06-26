/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.query;

import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;

/**
 * Annotates a field to be used in
 * {@link sirius.db.mixing.query.constraints.FilterFactory#queryString(EntityDescriptor, String, QueryField...)}.
 */
public class QueryField {

    /**
     * Determines the way, the filter value can be applied to a given field.
     */
    public enum Mode {
        EQUAL, LIKE, PREFIX, CONTAINS
    }

    private Mapping field;
    private Mode mode;

    private QueryField(Mapping field, Mode mode) {
        this.field = field;
        this.mode = mode;
    }

    /**
     * Informs the compiler, that only real equality checks are permitted for this field.
     * <p>
     * This ensures that a proper database index can be used for all queries.
     *
     * @param field the field to search in
     * @return the generated query field
     */
    public static QueryField eq(Mapping field) {
        return new QueryField(field, Mode.EQUAL);
    }

    /**
     * Informs the compiler, that by default, equality checks are used for this field.
     * <p>
     * However, if there are wildcards in the filter value, an expanding constraint can be generated.
     * <p>
     * This ensures that a proper database index can be used for common queries.
     *
     * @param field the field to search in
     * @return the generated query field
     */
    public static QueryField like(Mapping field) {
        return new QueryField(field, Mode.LIKE);
    }

    /**
     * Informs the compiler, that by default an expanding search like a prefix query may be generated for this field.
     * <p>
     * Depending on the underlying database an index might be used to support this query. If the database cannot
     * execute prefix queries if may fallback to an equals constraint.
     *
     * @param field the field to search in
     * @return the generated query field
     */
    public static QueryField startsWith(Mapping field) {
        return new QueryField(field, Mode.PREFIX);
    }

    /**
     * Informs the compiler, that by default a fully expanding search like a contains query may be generated for this field.
     * <p>
     * This will most probably disable any support for database indices and should only be used for reasonably sized datasets.
     *
     * @param field the field to search in
     * @return the generated query field
     */
    public static QueryField contains(Mapping field) {
        return new QueryField(field, Mode.CONTAINS);
    }

    public Mapping getField() {
        return field;
    }

    public Mode getMode() {
        return mode;
    }
}
