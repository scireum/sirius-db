/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.EntityDescriptor;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Provides a simple helper to generate DELETE statements with multiple where conditions on {@link SQLEntity entities}.
 * <p>
 * <b>Note that this will not execute any {@link sirius.db.mixing.annotations.BeforeDelete} or
 * {@link sirius.db.mixing.annotations.AfterDelete} handlers.</b>
 * <p>
 * This can be used to generate queries like {@code DELETE FROM table WHERE y = 'Value'} without having
 * to worry about typing field and table names correctly. Such deletes are sometimes more efficient as a lot of the
 * framework overhead is reduced (this is essentially just a builder which generates a {@link PreparedStatement}).
 */
public class DeleteStatement extends GeneratedStatement<DeleteStatement> {

    private static final String MICROTIMING_KEY = "DELETE";

    protected DeleteStatement(EntityDescriptor descriptor, Database db) {
        super(descriptor, db);
    }

    @Override
    protected void beginWHERE() {
        append("DELETE FROM ");
        append(descriptor.getRelationName());
        append(" WHERE ");
    }

    @Override
    protected String microtimingKey() {
        return MICROTIMING_KEY;
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (!wherePartStarted.isToggled()) {
            append("DELETE FROM ");
            append(descriptor.getRelationName());
        }
        return super.executeUpdate();
    }
}
