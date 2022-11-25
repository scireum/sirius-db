/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Reflection;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.nls.NLS;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Provides methods to compile statements with embedded parameters and optional blocks.
 */
class StatementCompiler {

    private PreparedStatement statement;
    private List<Tuple<Integer, Object>> parameters = new ArrayList<>();
    private Connection connection;
    private StringBuilder effectiveQueryBuilder;
    private boolean retrieveGeneratedKeys;
    private String originalSQL;
    private List<Object> params;
    private Context context;

    protected StatementCompiler(Connection connection, boolean retrieveGeneratedKeys) {
        this.connection = connection;
        this.retrieveGeneratedKeys = retrieveGeneratedKeys;
        this.effectiveQueryBuilder = new StringBuilder();
    }

    protected PreparedStatement getStatement() throws SQLException {
        if (statement == null) {
            if (retrieveGeneratedKeys) {
                statement =
                        connection.prepareStatement(effectiveQueryBuilder.toString(), Statement.RETURN_GENERATED_KEYS);
            } else {
                statement = connection.prepareStatement(effectiveQueryBuilder.toString(),
                                                        ResultSet.TYPE_FORWARD_ONLY,
                                                        ResultSet.CONCUR_READ_ONLY);
            }
            for (Tuple<Integer, Object> parameter : parameters) {
                try {
                    Databases.convertAndSetParameter(statement, parameter.getFirst(), parameter.getSecond());
                } catch (SQLException error) {
                    throw new SQLException(Strings.apply("Failed to set parameter %s to '%s': %s",
                                                         parameter.getFirst(),
                                                         parameter.getSecond(),
                                                         error.getMessage()), error);
                }
            }
        }
        return statement;
    }

    /**
     * Builds a PreparedStatement where references to parameters (${Param} for
     * normal substitution and #{Param} for LIKE substitution) are replaced by
     * the given parameters. Blocks created with [ and ] are taken out if the
     * parameter referenced in between is null.
     *
     * @param query   the query to compile
     * @param context the context defining the parameters available
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("""
            For performance reasons, we keep the original context around,
            as we do not use it outside the scope of this method.
            """)
    protected void buildParameterizedStatement(String query, Context context) throws SQLException {
        this.params = new ArrayList<>();
        if (query != null) {
            this.originalSQL = query;
            this.context = context;
            parseSection(query);
        }
        int index = 0;
        for (Object param : params) {
            if (param instanceof Collection<?>) {
                for (Object obj : (Collection<?>) param) {
                    parameters.add(Tuple.create(++index, obj));
                    Databases.LOG.FINE("SETTING: " + index + " TO " + NLS.toMachineString(obj));
                }
            } else {
                parameters.add(Tuple.create(++index, param));
                Databases.LOG.FINE("SETTING: " + index + " TO " + NLS.toMachineString(param));
            }
        }
    }

    /*
     * Searches for an occurrence of a block [ .. ]. Everything before the [ is
     * compiled and added to the result SQL. Everything between the brackets is
     * compiled and if a parameter was found it is added to the result SQL. The
     * part after the ] is parsed in a recursive call.
     * <p/>
     * If no [ was found, the complete string is compiled and added to the
     * result SQL.
     */
    private void parseSection(String sql) throws SQLException {
        int index = sql.indexOf('[');
        if (index > -1) {
            int nextClose = sql.indexOf(']', index + 1);
            if (nextClose < 0) {
                throw new SQLException(Strings.apply("Unbalanced [ at %d in: %s ", index, originalSQL));
            }
            int nextOpen = sql.indexOf('[', index + 1);
            if ((nextOpen > -1) && (nextOpen < nextClose)) {
                throw new SQLException(Strings.apply("Cannot nest blocks of angular brackets at %d in: %s ",
                                                     index,
                                                     originalSQL));
            }
            compileSection(false, sql.substring(0, index));
            compileSection(true, sql.substring(index + 1, nextClose));
            parseSection(sql.substring(nextClose + 1));
        } else {
            compileSection(false, sql);
        }
    }

    /*
     * Replaces all occurrences of parameters ${..} or #{..} by parameters given
     * in context.
     */
    private void compileSection(boolean ignoreIfParametersNull, String sql) throws SQLException {
        if (sql.startsWith(":")) {
            // Check if there is a condition present like [:enabled WHERE x = 1]
            sql = checkForBlockCondition(sql);
            if (sql == null) {
                // A condition is present and not fulfilled - skip this block...
                return;
            }

            if (getNextRelevantIndex(sql) == null) {
                // The condition was present and no other parameters are in the block, so we can append it
                // as is...
                effectiveQueryBuilder.append(sql);
                return;
            }
        }

        // Scan for parameters and only output if at least one is filled...
        List<Object> tempParams = new ArrayList<>();
        StringBuilder sqlBuilder = new StringBuilder();
        boolean appendToStatement = compileSectionPart(sql, tempParams, sqlBuilder, !ignoreIfParametersNull);

        if (appendToStatement) {
            effectiveQueryBuilder.append(sqlBuilder);
            params.addAll(tempParams);
        }
    }

    /*
     * Checks for a condition like [:fooMode AND y = 10 ].
     *
     * These will be cut and the whole block is only compiles if the condition (fooMode in this case) is a paremter
     * which is set to true.
     */
    private String checkForBlockCondition(String sql) throws SQLException {
        int endOfCondition = sql.indexOf(" ");
        if (endOfCondition < 0) {
            return sql;
        }

        String condition = sql.substring(1, endOfCondition);
        if (Boolean.TRUE.equals(computeEffectiveParameterValue(condition))) {
            return sql.substring(endOfCondition);
        } else {
            return null;
        }
    }

    private boolean compileSectionPart(String sql,
                                       List<Object> tempParams,
                                       StringBuilder sqlBuilder,
                                       boolean appendToStatement) throws SQLException {
        Tuple<Integer, Boolean> nextSubstitution = getNextRelevantIndex(sql);
        if (nextSubstitution == null) {
            if (appendToStatement) {
                sqlBuilder.append(sql);
            }

            return appendToStatement;
        }

        int endIndex = findClosingCurlyBracket(sql, nextSubstitution.getFirst());
        String parameterName = sql.substring(nextSubstitution.getFirst() + 2, endIndex);
        Object paramValue = computeEffectiveParameterValue(parameterName);

        if (Boolean.TRUE.equals(nextSubstitution.getSecond()) || paramValue == null) {
            tempParams.add(paramValue);
        } else {
            tempParams.add(addSQLWildcard(paramValue.toString().toLowerCase(), true));
        }

        sqlBuilder.append(sql, 0, nextSubstitution.getFirst());

        appendPlaceholdersToStatement(sqlBuilder, paramValue);

        return compileSectionPart(sql.substring(endIndex + 1),
                                  tempParams,
                                  sqlBuilder,
                                  appendToStatement || isParameterFilled(paramValue));
    }

    private Object computeEffectiveParameterValue(String fullParameterName) throws SQLException {
        String accessPath = null;
        String parameterName = fullParameterName;
        if (fullParameterName.contains(".")) {
            accessPath = parameterName.substring(parameterName.indexOf('.') + 1);
            parameterName = parameterName.substring(0, parameterName.indexOf('.'));
        }

        Object paramValue = context.get(parameterName);
        if (accessPath == null || paramValue == null) {
            if (!context.containsKey(parameterName)) {
                Databases.LOG.WARN("""
                                           SQL query references an unknown parameter: %s! \
                                           This is very likely an error and thus reported. \
                                           Set unknown parameters to null to suppress this warning!
                                           Query: %s
                                           Location:
                                           %s""",
                                   parameterName,
                                   this.originalSQL,
                                   ExecutionPoint.snapshot().toString());
            }
            return paramValue;
        }

        try {
            return Reflection.evalAccessPath(accessPath, paramValue);
        } catch (Exception e) {
            throw new SQLException(NLS.fmtr("StatementCompiler.cannotEvalAccessPath")
                                      .set("name", parameterName)
                                      .set("path", accessPath)
                                      .set("value", paramValue)
                                      .set("query", originalSQL)
                                      .format(), e);
        }
    }

    private void appendPlaceholdersToStatement(StringBuilder sqlBuilder, Object paramValue) {
        if (paramValue instanceof Collection<?>) {
            for (int i = 0; i < ((Collection<?>) paramValue).size(); i++) {
                if (i > 0) {
                    sqlBuilder.append(",");
                }
                sqlBuilder.append(" ? ");
            }
        } else {
            sqlBuilder.append(" ? ");
        }
    }

    private boolean isParameterFilled(Object paramValue) {
        if (paramValue == null) {
            return false;
        }

        if (paramValue instanceof Collection<?> collection) {
            return !collection.isEmpty();
        }

        if (paramValue instanceof String stringValue) {
            return stringValue.length() > 0;
        }

        return true;
    }

    private int findClosingCurlyBracket(String sql, int index) throws SQLException {
        int endIndex = sql.indexOf('}', index);
        if (endIndex < 0) {
            throw new SQLException(NLS.fmtr("StatementCompiler.errorUnbalancedCurlyBracket")
                                      .set("index", index)
                                      .set("query", originalSQL)
                                      .format());
        }
        return endIndex;
    }

    /*
     * Returns the next index of ${ or #{ in the given string.
     */
    @Nullable
    private Tuple<Integer, Boolean> getNextRelevantIndex(String sql) {
        int index = sql.indexOf("${");
        int sharpIndex = sql.indexOf("#{");
        if ((sharpIndex > -1) && ((index < 0) || (sharpIndex < index))) {
            return Tuple.create(sharpIndex, false);
        }
        if (index > -1) {
            return Tuple.create(index, true);
        } else {
            return null;
        }
    }

    /**
     * Make <tt>searchString</tt> conform with SQL 92 syntax. Therefore, all * are
     * converted to %, and a final % is appended at the end of the string.
     *
     * @param query        the query to expand
     * @param wildcardLeft determines if a % should be added to the start of the string
     * @return the query with appropriately escaped wildcards
     */
    public static String addSQLWildcard(String query, boolean wildcardLeft) {
        if (query == null) {
            return null;
        }
        if (Strings.isEmpty(query)) {
            return "%";
        }

        String result = query;
        if (!result.contains("%") && result.contains("*")) {
            result = result.replace('*', '%');
        }
        if (!result.endsWith("%")) {
            result = result + "%";
        }
        if (wildcardLeft && !result.startsWith("%")) {
            result = "%" + result;
        }
        return result;
    }
}
