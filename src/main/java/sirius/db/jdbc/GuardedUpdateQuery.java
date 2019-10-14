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
import sirius.kernel.commons.Watch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides a simple helper to generate efficient update queries on {@link SQLEntity entities}.
 * <p>
 * <b>Note that this will not execute any {@link sirius.db.mixing.annotations.BeforeSave} handlers.</b>
 * <p>
 * This can be used to generate queries like {@code UPDATE table SET x = 'Value' WHERE y = 'Value'} without having
 * to worry about typing field and table names correctly. Such update are sometimes more efficient as a lot of the
 * framework overhead is reduced (this is essentially just a builder which generates a {@link PreparedStatement}).
 * This can also be used for conditional updates (e.g. optimistic locking algorithms and the like.
 * <p>
 * Note that this, being a builder class with minimal state overhead, does not support to add another update
 * via {@link #set(Mapping, Object)} once the first constraint has been added via {@link #where(Mapping, Object)}.
 */
public class GuardedUpdateQuery {

    private static final String MICROTIMING_KEY = "SQL-GUARDED-UPDATE";
    private final EntityDescriptor descriptor;
    private final Database db;
    private StringBuilder queryBuilder = new StringBuilder();
    private boolean wherePart;
    private boolean setPart;
    private List<Object> parameters = new ArrayList<>();

    /**
     * Creates a new query instance.
     * <p>
     * Use {@link OMA#select(Class)} to create a new query.
     *
     * @param descriptor the descriptor of the type to query
     * @param db         the database to operate on
     */
    protected GuardedUpdateQuery(EntityDescriptor descriptor, Database db) {
        this.descriptor = descriptor;
        this.db = db;
    }

    /**
     * Specifies a field to update with the given value.
     *
     * @param field the field to update
     * @param value the value to place in the field
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery set(Mapping field, Object value) {
        prepareSet();
        append(determineEffectiveColumnName(field));
        append(" = ?");
        parameters.add(value);
        return this;
    }

    private void prepareSet() {
        if (wherePart) {
            throw new IllegalStateException("Cannot append to the SET part when already building the WHERE part");
        }
        if (!setPart) {
            setPart = true;
            append("UPDATE ");
            append(descriptor.getRelationName());
            append(" SET ");
        } else {
            append(", ");
        }
    }

    private void append(String sqlPart) {
        if (queryBuilder == null) {
            throw new IllegalStateException("Cannot modify an already executed query.");
        }

        queryBuilder.append(sqlPart);
    }

    /**
     * Increments the given field by one.
     * <p>
     * This is an atomic operation as it is done via a generate SQL expression.
     *
     * @param field the field to increment
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery inc(Mapping field) {
        prepareSet();
        String columnName = determineEffectiveColumnName(field);
        append(columnName);
        append(" = ");
        append(columnName);
        append(" + 1");
        return this;
    }

    /**
     * Specifies a field to update with the given value if a given condition is filled.
     * <p>
     * This is a boilerplate method which can simplify some code fragments as some updates depend on checks or
     * parameters.
     *
     * @param field     the field to update
     * @param value     the value to place in the field
     * @param condition the condition which needs to be fulfilled (<tt>true</tt>) in order to actually put the value
     *                  in the field.
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery setIf(Mapping field, Object value, boolean condition) {
        if (condition) {
            set(field, value);
        }

        return this;
    }

    /**
     * Specifies a field to update with the given value (as long as it isn't <tt>null</tt>).
     *
     * @param field the field to update
     * @param value the value to place in the field
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery setIgnoringNull(Mapping field, Object value) {
        return setIf(field, value, value != null);
    }

    /**
     * Specifies a field to be filled with the current {@link LocalDateTime}.
     *
     * @param field the field to update
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery setToNow(Mapping field) {
        return set(field, LocalDateTime.now());
    }

    /**
     * Specifies a field to be filled with the current {@link LocalDate}.
     *
     * @param field the field to update
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery setToToday(Mapping field) {
        return set(field, LocalDate.now());
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
    public GuardedUpdateQuery where(Mapping field, Object value) {
        prepareWhere();
        append(determineEffectiveColumnName(field));
        if (value == null) {
            append(" IS NULL");
        } else {
            append(" = ?");
            parameters.add(value);
        }
        return this;
    }

    private String determineEffectiveColumnName(Mapping field) {
        if (field.getParent() != null) {
            throw new IllegalArgumentException(
                    "A GuardedUpdateQuery doesn't support automatic joins. Offending column: " + field);
        }
        return descriptor.getProperty(field.getName()).getPropertyName();
    }

    private void prepareWhere() {
        if (!wherePart) {
            wherePart = true;
            append(" WHERE ");
        } else {
            append(", ");
        }
    }

    /**
     * Adds an equals constraint to the query is the given condition is fulfilled (<tt>true</tt>).
     *
     * @param field     the field to check
     * @param value     the value to enforce
     * @param condition the condition which must be <tt>true</tt> in order to create the constraint
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery whereIf(Mapping field, Object value, boolean condition) {
        if (condition) {
            where(field, value);
        }

        return this;
    }

    /**
     * Adds an equals constraint to the query unless the given value is <tt>null</tt>.
     *
     * @param field the field to check
     * @param value the value to enforce
     * @return the query itself for fluent method calls
     */
    public GuardedUpdateQuery whereIgnoreNull(Mapping field, Object value) {
        return whereIf(field, value, value != null);
    }

    /**
     * Executes the update against the database.
     *
     * @return the number of updated entities / rows
     * @throws SQLException in case of a database error
     */
    public int executeUpdate() throws SQLException {
        if (!setPart) {
            return 0;
        }

        String sql = queryBuilder.toString();
        queryBuilder = null;

        Watch watch = Watch.start();
        try (Connection c = db.getConnection()) {
            try (PreparedStatement stmt = c.prepareStatement(sql)) {
                for (int i = 0; i < parameters.size(); i++) {
                    stmt.setObject(i + 1, parameters.get(i));
                }
                return stmt.executeUpdate();
            }
        } finally {
            watch.submitMicroTiming(MICROTIMING_KEY, sql);
        }
    }
}
