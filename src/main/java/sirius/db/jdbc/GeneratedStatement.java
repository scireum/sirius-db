/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Watch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides a common set of functions used by the builders {@link UpdateStatement} and {@link DeleteStatement}.
 *
 * @param <S> the effective statement class to support fluent method calls
 */
abstract class GeneratedStatement<S extends GeneratedStatement<S>> {

    /**
     * Contains the descriptor of the entities being modified.
     */
    protected final EntityDescriptor descriptor;

    /**
     * Contains the underlying database.
     */
    protected final Database db;

    /**
     * Builds the effective SQL statement to execute.
     */
    protected StringBuilder queryBuilder = new StringBuilder();

    /**
     * Determines if the WHERE part has already been started.
     */
    protected final Monoflop wherePartStarted = Monoflop.create();

    /**
     * Contains the list of parameters to pass into the generated {@link PreparedStatement}.
     */
    protected final List<Object> parameters = new ArrayList<>();

    /**
     * Enumerates the operators supported by {@link #where(Mapping, Operator, Object)}.
     */
    public enum Operator {
        LT("<"), LT_EQ("<="), EQ("="), GT_EQ(">="), GT(">"), NE("<>");

        private final String operation;

        Operator(String operation) {
            this.operation = operation;
        }

        @Override
        public String toString() {
            return operation;
        }
    }

    /**
     * Creates a new instance for the given descriptor and database.
     *
     * @param descriptor the descriptor of the entities being modified
     * @param db         the database to operate on
     */
    protected GeneratedStatement(EntityDescriptor descriptor, Database db) {
        this.descriptor = descriptor;
        this.db = db;
    }

    /**
     * Appends a strng to the resulting SQL statement.
     *
     * @param sqlPart the string to append
     */
    protected void append(String sqlPart) {
        if (queryBuilder == null) {
            throw new IllegalStateException("Cannot modify an already executed query.");
        }

        queryBuilder.append(sqlPart);
    }

    /**
     * Adds the given constraint to the query.
     *
     * @param field the field to check
     * @param op    the operator to use
     * @param value the value to enforce
     * @return the query itself for fluent method calls
     */
    public S where(Mapping field, Operator op, Object value) {
        prepareWhere();
        append(determineEffectiveColumnName(field));
        if (value == null && op == Operator.EQ) {
            append(" IS NULL");
        } else if (value == null && op == Operator.NE) {
            append(" IS NOT NULL");
        } else {
            append(" ");
            append(op.toString());
            append(" ?");

            parameters.add(value);
        }
        return self();
    }

    /**
     * Helper method for fluent method calls on the generic class
     *
     * @return <tt>this</tt> casted to the proper subclass
     */
    @SuppressWarnings("unchecked")
    protected S self() {
        return (S) this;
    }

    /**
     * Adds an equals constraint to the query.
     * <p>
     * This will essentially generate a condition like {@code field = value}.
     *
     * @param field the field to check
     * @param value the value to enforce
     * @return the query itself for fluent method calls
     */
    public S where(Mapping field, Object value) {
        return where(field, Operator.EQ, value);
    }

    /**
     * Determines if column name to use for a given property.
     *
     * @param field the name of the property to access
     * @return the effective column name of the given property / field
     */
    protected String determineEffectiveColumnName(Mapping field) {
        if (field.getParent() != null) {
            throw new IllegalArgumentException(
                    "A GeneratedStatement doesn't support automatic joins. Offending column: " + field);
        }
        return descriptor.getProperty(field.getName()).getPropertyName();
    }

    /**
     * Prepares the resulting SQL to add another constraint to the WHERE part.
     */
    protected void prepareWhere() {
        if (wherePartStarted.firstCall()) {
            beginWHERE();
        } else {
            append(", ");
        }
    }

    /**
     * Emits the SQL which is required to start the WHERE part
     */
    protected void beginWHERE() {
        append(" WHERE ");
    }

    /**
     * Adds an equals constraint to the query is the given condition is fulfilled (<tt>true</tt>).
     *
     * @param field     the field to check
     * @param value     the value to enforce
     * @param condition the condition which must be <tt>true</tt> in order to create the constraint
     * @return the query itself for fluent method calls
     */
    public S whereIf(Mapping field, Object value, boolean condition) {
        if (condition) {
            where(field, Operator.EQ, value);
        }

        return self();
    }

    /**
     * Adds an equals constraint to the query unless the given value is <tt>null</tt>.
     *
     * @param field the field to check
     * @param value the value to enforce
     * @return the query itself for fluent method calls
     */
    public S whereIgnoreNull(Mapping field, Object value) {
        return whereIf(field, value, value != null);
    }

    /**
     * Executes the statement against the database.
     *
     * @return the number of updated entities / rows
     * @throws SQLException in case of a database error
     */
    public int executeUpdate() throws SQLException {
        String sql = queryBuilder.toString();
        queryBuilder = null;

        Watch watch = Watch.start();
        try (Connection c = db.getConnection()) {
            try (PreparedStatement stmt = c.prepareStatement(sql)) {
                for (int i = 0; i < parameters.size(); i++) {
                    stmt.setObject(i + 1, Databases.convertValue(parameters.get(i)));
                }

                return stmt.executeUpdate();
            }
        } finally {
            watch.submitMicroTiming(microtimingKey(), sql);
        }
    }

    /**
     * Returns the {@link sirius.kernel.health.Microtiming} key to used for this statement type.
     *
     * @return the microtiming key to use
     */
    protected abstract String microtimingKey();
}
