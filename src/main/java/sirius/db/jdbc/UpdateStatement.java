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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Provides a simple helper to generate UPDATE statements with multiple where conditions on {@link SQLEntity entities}.
 * <p>
 * <b>Note that this will not execute any {@link sirius.db.mixing.annotations.BeforeSave} or
 * {@link sirius.db.mixing.annotations.AfterSave} handlers.</b>
 * <p>
 * This can be used to generate queries like {@code UPDATE table SET x = 'Value' WHERE y = 'Value'} without having
 * to worry about typing field and table names correctly. Such update are sometimes more efficient as a lot of the
 * framework overhead is reduced (this is essentially just a builder which generates a {@link PreparedStatement}).
 * This can also be used for conditional updates (e.g. optimistic locking algorithms and the like.
 * <p>
 * Note that this, being a builder class with minimal state overhead, does not support to add another update
 * via {@link #set(Mapping, Object)} once the first constraint has been added via {@link #where(Mapping, Object)}.
 */
public class UpdateStatement extends GeneratedStatement<UpdateStatement> {

    private static final String MICROTIMING_KEY = "UPDATE";
    private final Monoflop setPartStarted = Monoflop.create();

    protected UpdateStatement(EntityDescriptor descriptor, Database db) {
        super(descriptor, db);
    }

    @Override
    protected String microtimingKey() {
        return MICROTIMING_KEY;
    }

    /**
     * Specifies a field to update with the given value.
     *
     * @param field the field to update
     * @param value the value to place in the field
     * @return the query itself for fluent method calls
     */
    public UpdateStatement set(Mapping field, Object value) {
        prepareSet();
        append(determineEffectiveColumnName(field));
        append(" = ?");
        parameters.add(value);
        return this;
    }

    private void prepareSet() {
        if (wherePartStarted.isToggled()) {
            throw new IllegalStateException("Cannot append to the SET part when already building the WHERE part");
        }
        if (setPartStarted.firstCall()) {
            append("UPDATE ");
            append(descriptor.getRelationName());
            append(" SET ");
        } else {
            append(", ");
        }
    }

    /**
     * Increments the given field by one.
     * <p>
     * This is an atomic operation as it is done via a generate SQL expression.
     *
     * @param field the field to increment
     * @return the query itself for fluent method calls
     */
    public UpdateStatement inc(Mapping field) {
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
    public UpdateStatement setIf(Mapping field, Object value, boolean condition) {
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
    public UpdateStatement setIgnoringNull(Mapping field, Object value) {
        return setIf(field, value, value != null);
    }

    /**
     * Specifies a field to be filled with the current {@link LocalDateTime}.
     *
     * @param field the field to update
     * @return the query itself for fluent method calls
     */
    public UpdateStatement setToNow(Mapping field) {
        return set(field, LocalDateTime.now());
    }

    /**
     * Specifies a field to be filled with the current {@link LocalDate}.
     *
     * @param field the field to update
     * @return the query itself for fluent method calls
     */
    public UpdateStatement setToToday(Mapping field) {
        return set(field, LocalDate.now());
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (!setPartStarted.isToggled()) {
            return 0;
        }

        return super.executeUpdate();
    }
}
