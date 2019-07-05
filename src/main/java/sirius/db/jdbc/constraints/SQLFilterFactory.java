/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.Databases;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.query.constraints.FilterFactory;

import java.util.List;

/**
 * Generates filters and constraints for {@link sirius.db.jdbc.SmartQuery}.
 *
 * @see sirius.db.jdbc.OMA#FILTERS
 */
public class SQLFilterFactory extends FilterFactory<SQLConstraint> {

    @Override
    protected Object customTransform(Object value) {
        return Databases.convertValue(value);
    }

    @Override
    protected SQLConstraint eqValue(Mapping field, Object value) {
        return new FieldOperator(field, "=", value);
    }

    @Override
    protected SQLConstraint neValue(Mapping field, Object value) {
        return new FieldOperator(field, "<>", value);
    }

    @Override
    protected SQLConstraint gtValue(Mapping field, Object value, boolean orEqual) {
        return new FieldOperator(field, orEqual ? ">=" : ">", value);
    }

    @Override
    protected SQLConstraint ltValue(Mapping field, Object value, boolean orEqual) {
        return new FieldOperator(field, orEqual ? "<=" : "<", value);
    }

    @Override
    public SQLConstraint filled(Mapping field) {
        return new Filled(field);
    }

    /**
     * Ensures that the given field is neither <tt>NULL</tt> nor an empty string.
     *
     * @param field the field to check
     * @return a constraint which ensures that the given field contains a non-empty string
     */
    public SQLConstraint nonEmptyString(Mapping field) {
        return and(new Filled(field), ne(field, ""));
    }

    @Override
    public SQLConstraint notFilled(Mapping field) {
        return new NotFilled(field);
    }

    @Override
    public SQLConstraint isEmptyArray(Mapping field) {
        return notFilled(field);
    }

    @Override
    protected SQLConstraint invert(SQLConstraint constraint) {
        return new Not(constraint);
    }

    @Override
    protected SQLConstraint effectiveAnd(List<SQLConstraint> effectiveConstraints) {
        return new And(effectiveConstraints);
    }

    @Override
    protected SQLConstraint effectiveOr(List<SQLConstraint> effectiveConstraints) {
        return new Or(effectiveConstraints);
    }

    @Override
    public SQLConstraint queryString(EntityDescriptor descriptor, String query, List<QueryField> fields) {
        return new SQLQueryCompiler(this, descriptor, query, fields).compile();
    }

    /**
     * Generates an EXISTS clause: <tt>EXISTS(SELECT * FROM other WHERE e.outerColumn = other.innerColumn</tt>.
     *
     * @param outerColumn the column of the entities being queried to match
     * @param other       the entity type to search in (which must exist)
     * @param innerColumn the column within that inner entity type which must match the outer column
     * @return an exists constraint which can be filtered with additional constraints
     */
    public Exists existsIn(Mapping outerColumn, Class<? extends SQLEntity> other, Mapping innerColumn) {
        return new Exists(outerColumn, other, innerColumn);
    }

    /**
     * Creates a LIKE constraint for the given field.
     *
     * @param fields the field to search in
     * @return a builder used to construct a LIKE constraint
     */
    public Like like(Mapping fields) {
        return new Like(fields);
    }
}
