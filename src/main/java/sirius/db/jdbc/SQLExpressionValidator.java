/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.commons.Strings;

import java.util.Set;

/**
 * Validates SQL expressions which are directly embedded into generated SQL statements.
 */
public class SQLExpressionValidator {

    private static final Set<String> FORBIDDEN_SQL_EXPRESSION_PARTS = Set.of("--", "/*", "*/", ";");
    private static final Set<String> FORBIDDEN_SQL_EXPRESSION_KEYWORDS = Set.of("ALTER",
                                                                                "CALL",
                                                                                "CREATE",
                                                                                "DELETE",
                                                                                "DROP",
                                                                                "EXEC",
                                                                                "EXECUTE",
                                                                                "FROM",
                                                                                "HAVING",
                                                                                "INSERT",
                                                                                "INTO",
                                                                                "JOIN",
                                                                                "LIMIT",
                                                                                "MERGE",
                                                                                "OFFSET",
                                                                                "ORDER",
                                                                                "SELECT",
                                                                                "TRUNCATE",
                                                                                "UNION",
                                                                                "UPDATE",
                                                                                "VALUES",
                                                                                "WHERE",
                                                                                "WITH");

    private SQLExpressionValidator() {
    }

    /**
     * Validates an SQL expression against a basic SQL injection blocklist.
     *
     * @param expression the SQL expression to validate
     * @throws IllegalArgumentException if the expression is empty or contains blocked SQL syntax
     */
    public static void assertAllowedSQLExpression(String expression) {
        if (Strings.isEmpty(expression)) {
            throw new IllegalArgumentException("Expression must not be empty.");
        }

        if (containsForbiddenSQLExpressionPart(expression) || containsForbiddenSQLKeyword(expression)) {
            throw new IllegalArgumentException("Expression contains forbidden SQL syntax: " + expression);
        }
    }

    private static boolean containsForbiddenSQLExpressionPart(String expression) {
        for (String forbiddenPart : FORBIDDEN_SQL_EXPRESSION_PARTS) {
            if (expression.contains(forbiddenPart)) {
                return true;
            }
        }

        return false;
    }

    private static boolean containsForbiddenSQLKeyword(String expression) {
        int index = 0;
        while (index < expression.length()) {
            if (isSQLIdentifierPart(expression.charAt(index))) {
                int end = determineIdentifierEnd(expression, index);
                String identifier = expression.substring(index, end).toUpperCase();
                if (FORBIDDEN_SQL_EXPRESSION_KEYWORDS.contains(identifier)) {
                    return true;
                }
                index = end;
            } else {
                index++;
            }
        }

        return false;
    }

    private static int determineIdentifierEnd(String expression, int start) {
        int end = start + 1;

        while (end < expression.length() && isSQLIdentifierPart(expression.charAt(end))) {
            end++;
        }

        return end;
    }

    private static boolean isSQLIdentifierPart(char current) {
        return Character.isLetterOrDigit(current) || current == '_';
    }
}
