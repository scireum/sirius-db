/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.commons.Value;
import sirius.kernel.commons.Watch;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a flexible way of executing parameterized SQL calls without
 * thinking too much about resource management.
 */
public class SQLCall {

    private static final String RETURN_VALUE = "_RETVAL";
    private final Database ds;
    private final List<String> names = new ArrayList<>();
    private final List<Object> data = new ArrayList<>();
    private final List<Integer> types = new ArrayList<>();
    private final String fun;
    private final Map<String, Object> output = new TreeMap<>();
    private Integer returnType;

    /*
     * Use Database.createFunctionCall or Database.createFunctionCall to create an instance
     */
    protected SQLCall(Database ds, String fun, Integer returnType) {
        this.ds = ds;
        this.fun = fun;
        this.returnType = returnType;
        if (returnType != null) {
            addOutParam(RETURN_VALUE, returnType);
        }
    }

    /**
     * Adds an in parameter.
     *
     * @param value the value to pass in
     * @return the call itself to iterate fluent calls
     */
    public SQLCall addInParam(Object value) {
        names.add("");
        data.add(value);
        types.add(null);
        return this;
    }

    /**
     * Adds an out parameter.
     *
     * @param parameter the name of the parameter
     * @param type      the SQL type ({@link java.sql.Types}) of the parameter
     * @return the call itself to iterate fluent calls
     */
    public SQLCall addOutParam(String parameter, int type) {
        names.add(parameter);
        data.add(null);
        types.add(type);
        return this;
    }

    /**
     * Invokes the call
     *
     * @return the call itself to iterate fluent calls in order to read the result
     * @throws SQLException in case of a database error
     */
    public SQLCall call() throws SQLException {
        Watch w = Watch.start();
        try (Connection c = ds.getConnection()) {
            StringBuilder sql = new StringBuilder("{call ");
            if (returnType != null) {
                sql.append("? := ");
            }
            sql.append(fun);
            sql.append("(");
            for (int i = returnType != null ? 1 : 0; i < names.size(); i++) {
                if (i > (returnType != null ? 1 : 0)) {
                    sql.append(", ");
                }
                sql.append("?");
            }
            sql.append(")}");

            try (CallableStatement stmt = c.prepareCall(sql.toString())) {
                writeParameters(stmt);
                stmt.execute();
                readBackParameters(stmt);
            }
        } finally {
            w.submitMicroTiming("SQL", "CALL: " + fun);
        }

        return this;
    }

    protected void writeParameters(CallableStatement stmt) throws SQLException {
        for (int i = 0; i < names.size(); i++) {
            if (types.get(i) != null) {
                stmt.registerOutParameter(i + 1, types.get(i));
            } else {
                stmt.setObject(i + 1, data.get(i));
            }
        }
    }

    protected void readBackParameters(CallableStatement stmt) throws SQLException {
        for (int i = 0; i < names.size(); i++) {
            if (types.get(i) != null) {
                output.put(names.get(i), stmt.getObject(i + 1));
            }
        }
    }

    /**
     * Returns the return value of the function call
     *
     * @return the return value wrapped as {@link Value}
     */
    public Value getReturnValue() {
        return getValue(RETURN_VALUE);
    }

    /**
     * Returns the named out parameter
     *
     * @param key the parameter to return
     * @return the value of the given out parameter wrapped as {@link Value}
     */
    public Value getValue(String key) {
        return Value.of(output.get(key));
    }

    @Override
    public String toString() {
        return "JDBCCall [" + fun + "]";
    }
}
