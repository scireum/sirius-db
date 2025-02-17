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

    private final Mapping field;
    private final Mode mode;
    private final boolean caseSensitive;

    private QueryField(Mapping field, Mode mode, boolean caseSensitive) {
        this.field = field;
        this.mode = mode;
        this.caseSensitive = caseSensitive;
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
        return new QueryField(field, Mode.EQUAL, true);
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
        return new QueryField(field, Mode.LIKE, true);
    }

    /**
     * Informs the compiler, that by default, equality checks are used for this field.
     * <p>
     * However, if there are wildcards in the filter value, an expanding constraint can be generated.
     * <p>
     * Caveat: This method is case-insensitive and therefore will <b>not use an index</b> and should only be used for reasonably sized datasets.
     *
     * @param field the field to search in
     * @return the generated query field
     */
    public static QueryField likeIgnoreCase(Mapping field) {
        return new QueryField(field, Mode.LIKE, false);
    }


    /**
     * Informs the compiler, that by default an expanding search like a prefix query may be generated for this field.
     * <p>
     * Depending on the underlying database an index might be used to support this query. If the database cannot
     * execute prefix queries it may fall back to an equals-constraint.
     *
     * @param field the field to search in
     * @return the generated query field
     */
    public static QueryField startsWith(Mapping field) {
        return new QueryField(field, Mode.PREFIX, true);
    }

    /**
     * Informs the compiler, that by default an expanding search like a case-insensitive prefix query may be generated for this field.
     * <p>
     * Depending on the underlying database an index might be used to support this query. If the database cannot
     * execute prefix queries it may fall back to an equals-constraint.
     * <p>
     * Caveat: This method is case-insensitive and therefore will <b>not use an index</b> and should only be used for reasonably sized datasets.
     *
     * @param field the field to search in
     * @return the generated query field
     */
    public static QueryField startsWithIgnoreCase(Mapping field) {
        return new QueryField(field, Mode.PREFIX, false);
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
        return new QueryField(field, Mode.CONTAINS, true);
    }

    public Mapping getField() {
        return field;
    }

    public Mode getMode() {
        return mode;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }
}
