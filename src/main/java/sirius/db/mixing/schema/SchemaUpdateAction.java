/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.schema;

import sirius.db.jdbc.Database;
import sirius.kernel.async.TaskContext;
import sirius.db.mixing.OMA;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Represents an action which needs to be perfomed onto a schema to make it
 * match another schema.
 *
 * @author aha
 */
public class SchemaUpdateAction {

    private String id = UUID.randomUUID().toString();
    private String reason;
    private List<String> sql;
    private boolean dataLossPossible;
    private String error;
    private volatile boolean executed;

    public String getReason() {
        return reason;
    }

    protected void setReason(String reason) {
        this.reason = reason;
    }

    public List<String> getSql() {
        return sql;
    }

    protected void setSql(String sql) {
        this.sql = Collections.singletonList(sql);
    }

    protected void setSql(List<String> sql) {
        this.sql = sql;
    }

    public boolean isDataLossPossible() {
        return dataLossPossible;
    }

    protected void setDataLossPossible(boolean dataLossPossible) {
        this.dataLossPossible = dataLossPossible;
    }

    public String getId() {
        return id;
    }

    public String getError() {
        return error;
    }

    public void execute(Database db) {
        error = null;
        for (String statement : getSql()) {
            if (TaskContext.get().isActive()) {
                try {
                    OMA.LOG.FINE("Executing Schema Update: %s", statement);
                    db.createQuery(statement).executeUpdate();
                } catch (SQLException e) {
                    error = e.getMessage();
                }
            }
        }
        executed = !isFailed();
    }

    public boolean isFailed() {
        return error != null;
    }

    public boolean isExecuted() {
        return executed;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getReason());
        for (String statement : sql) {
            sb.append("\n");
            sb.append(statement);
        }

        if (error != null) {
            sb.append("\n");
            sb.append("Error: ");
            sb.append(error);
        }

        return sb.toString();
    }
}
