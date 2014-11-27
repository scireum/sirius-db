/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Reflection;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.nls.NLS;

import java.sql.SQLException;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.*;

/**
 * Provides methods to compile statements with embedded parameters and optional blocks.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/11
 */
class StatementCompiler {

    /**
     * Builds a PreparedStatement where references to parameters (${Param} for
     * normal substitution and #{Param} for LIKE substitution) are replaced by
     * the given parameters. Blocks created with [ and ] are taken out if the
     * parameter referenced in between is null.
     *
     * @param adapter used to determine the dialect to generate (SQL, HQL...)
     * @param query   the query to compile
     * @param context the context defining the parameters available
     */
    static void buildParameterizedStatement(StatementStrategy adapter,
                                            String query,
                                            Context context) throws SQLException {
        List<Object> params = new ArrayList<Object>();
        if (query != null) {
            parseSection(query, query, adapter, params, context);
        }
        try {
            int index = 0;
            for (Object param : params) {
                if (param instanceof Collection<?>) {
                    for (Object obj : (Collection<?>) param) {
                        if ((obj != null) && (obj instanceof TemporalAccessor)) {
                            adapter.set(++index, Date.from(Value.of(obj).asLocalDateTime(null).atZone(ZoneId.systemDefault()).toInstant()));
                        } else {
                            adapter.set(++index, obj);
                        }
                        Databases.LOG.FINE("SETTING: " + index + " TO " + NLS.toMachineString(obj));
                    }
                } else {
                    if ((param != null) && (param instanceof TemporalAccessor)) {
                        adapter.set(++index, Date.from(Value.of(param).asLocalDateTime(null).atZone(ZoneId.systemDefault()).toInstant()));
                    } else {
                        adapter.set(++index, param);
                    }
                    Databases.LOG.FINE("SETTING: " + index + " TO " + NLS.toMachineString(param));
                }
            }
        } catch (SQLException e) {
            throw new SQLException(Strings.apply("Error compiling: %s - %s", adapter.getQueryString(), e.getMessage()));
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
    private static void parseSection(String originalSQL,
                                     String sql,
                                     StatementStrategy adapter,
                                     List<Object> params,
                                     Context context) throws SQLException {
        int index = sql.indexOf("[");
        if (index > -1) {
            int nextClose = sql.indexOf("]", index + 1);
            if (nextClose < 0) {
                throw new SQLException(Strings.apply("Unbalanced [ at %d in: %s ", index, originalSQL));
            }
            int nextOpen = sql.indexOf("[", index + 1);
            if ((nextOpen > -1) && (nextOpen < nextClose)) {
                throw new SQLException(Strings.apply("Cannot nest blocks of angular brackets at %d in: %s ",
                        index,
                        originalSQL));
            }
            compileSection(false, sql, sql.substring(0, index), adapter, params, context);
            compileSection(true, sql, sql.substring(index + 1, nextClose), adapter, params, context);
            parseSection(originalSQL, sql.substring(nextClose + 1), adapter, params, context);
        } else {
            compileSection(false, sql, sql, adapter, params, context);
        }
    }

    /**
     * Make <tt>searchString</tt> conform with SQL 92 syntax. Therefore all * are
     * converted to % and a final % is appended at the end of the string.
     *
     * @param searchString the query to expand
     * @param wildcardLeft determines if a % should be added to the start of the string
     */
    public static String addSQLWildcard(String searchString, boolean wildcardLeft) {
        if (searchString == null) {
            return null;
        }
        if (searchString == "") {
            return "%";
        }
        if ((!searchString.contains("%")) && (searchString.contains("*"))) {
            searchString = searchString.replace('*', '%');
        }
        if (!searchString.endsWith("%")) {
            searchString = searchString + "%";
        }
        if (wildcardLeft && !searchString.startsWith("%")) {
            searchString = "%" + searchString;
        }
        return searchString;
    }

    /*
     * Replaces all occurrences of parameters ${..} or #{..} by parameters given
     * in context.
     */
    @SuppressWarnings("unchecked")
    private static void compileSection(boolean ignoreIfParametersNull,
                                       String originalSQL,
                                       String sql,
                                       StatementStrategy adapter,
                                       List<Object> params,
                                       Map<String, Object> context) throws SQLException {
        boolean parameterFound = !ignoreIfParametersNull;
        List<Object> tempParams = Lists.newArrayList();
        StringBuilder sqlBuilder = new StringBuilder();
        int index = getNextRelevantIndex(sql);
        boolean directSubstitution = (index > 0) && (sql.charAt(index) == '$');
        while (index >= 0) {
            int endIndex = sql.indexOf("}", index);
            if (endIndex < 0) {
                throw new SQLException(NLS.fmtr("StatementCompiler.errorUnbalancedCurlyBracket")
                        .set("index", index)
                        .set("query", originalSQL)
                        .format());
            }
            String parameterName = sql.substring(index + 2, endIndex);
            String accessPath = null;
            if (parameterName.contains(".")) {
                accessPath = parameterName.substring(parameterName.indexOf(".") + 1);
                parameterName = parameterName.substring(0, parameterName.indexOf("."));
            }

            Object paramValue = context.get(parameterName);
            if (accessPath != null && paramValue != null) {
                try {
                    paramValue = Reflection.evalAccessPath(accessPath, paramValue);
                } catch (Throwable e) {
                    throw new SQLException(NLS.fmtr("StatementCompiler.cannotEvalAccessPath")
                            .set("name", parameterName)
                            .set("path", accessPath)
                            .set("value", paramValue)
                            .set("query", originalSQL)
                            .format());
                }
            }
            // A parameter was found, if its value is not null or if it is a non
            // empty collection
            parameterFound = (Strings.isFilled(paramValue)) && (!(paramValue instanceof Collection<?>) || !((Collection<?>) paramValue)
                    .isEmpty());
            if (directSubstitution || paramValue == null) {
                tempParams.add(paramValue);
            } else {
                tempParams.add(addSQLWildcard(paramValue.toString().toLowerCase(), true));
            }
            sqlBuilder.append(sql.substring(0, index));
            if (paramValue instanceof Collection<?>) {
                for (int i = 0; i < ((Collection<?>) paramValue).size(); i++) {
                    if (i > 0) {
                        sqlBuilder.append(",");
                    }
                    sqlBuilder.append(" ");
                    sqlBuilder.append(adapter.getParameterName());
                    sqlBuilder.append(" ");
                }
            } else {
                sqlBuilder.append(" ");
                sqlBuilder.append(adapter.getParameterName());
                sqlBuilder.append(" ");
            }
            sql = sql.substring(endIndex + 1);
            index = getNextRelevantIndex(sql);
            directSubstitution = (index > 0) && (sql.charAt(index) == '$');
        }
        sqlBuilder.append(sql);
        if (parameterFound || !ignoreIfParametersNull) {
            adapter.appendString(sqlBuilder.toString());
            params.addAll(tempParams);
        }
    }

    /*
     * Returns the next index of ${ or #{ in the given string.
     */
    private static int getNextRelevantIndex(String sql) {
        int index = sql.indexOf("${");
        int sharpIndex = sql.indexOf("#{");
        if ((sharpIndex > -1) && ((index < 0) || (sharpIndex < index))) {
            return sharpIndex;
        }
        return index;
    }
}
