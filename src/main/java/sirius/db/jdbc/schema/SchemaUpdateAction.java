/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.db.jdbc.Database;
import sirius.db.jdbc.OMA;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Represents an action which needs to be perfomed onto a schema to make it
 * match another schema.
 */
public class SchemaUpdateAction {

    private String id = UUID.randomUUID().toString();
    private String reason;
    private List<String> sql;
    private boolean dataLossPossible;
    private String error;
    private String realm;
    private volatile boolean executed;

    /**
     * Creates a new schema update action.
     *
     * @param realm the realm (database) for which this change is generated
     */
    public SchemaUpdateAction(String realm) {
        this.realm = realm;
    }

    public String getRealm() {
        return realm;
    }

    protected void setRealm(String realm) {
        this.realm = realm;
    }

    /**
     * Contains a short description of what the change will cause.
     *
     * @return a short description of the change
     */
    public String getReason() {
        return reason;
    }

    protected void setReason(String reason) {
        this.reason = reason;
    }

    /**
     * A list of SQL statements which have to be executed to perform the required change.
     *
     * @return a list of SQL statement which make up the change
     */
    public List<String> getSql() {
        return Collections.unmodifiableList(sql);
    }

    protected void setSql(String sql) {
        this.sql = Collections.singletonList(sql);
    }

    protected void setSql(List<String> sql) {
        this.sql = sql;
    }

    /**
     * Determines if dataloss is possible when executing this change.
     *
     * @return <tt>true</tt> if dataloss is possible (a column or table is dropped), <tt>false</tt> otherwise
     */
    public boolean isDataLossPossible() {
        return dataLossPossible;
    }

    protected void setDataLossPossible(boolean dataLossPossible) {
        this.dataLossPossible = dataLossPossible;
    }

    /**
     * Returns an unique ID which identifies this change
     *
     * @return a unique ID for this change
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the last error reported by the database.
     *
     * @return an error message, if the change was already executed and failed, <tt>null</tt> otherwise
     */
    public String getError() {
        return error;
    }

    /**
     * Executes the change against the given database.
     *
     * @param db the datbase to change
     */
    public void execute(Database db) {
        error = null;
        for (String statement : getSql()) {
                try {
                    OMA.LOG.FINE("Executing Schema Update: %s", statement);
                    db.createQuery(statement).executeUpdate();
                } catch (Exception e) {
                    error = e.getMessage();
                }
            }
        }
        executed = !isFailed();
    }

    /**
     * Determines if the change was successfully executed.
     *
     * @return <tt>false</tt> if the last execution of this change failed, <tt>false</tt> otherwise
     */
    public boolean isFailed() {
        return error != null;
    }

    /**
     * Determines if the change was already executed.
     *
     * @return <tt>true</tt> if the change was executed, <tt>false</tt> otherwise
     */
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
