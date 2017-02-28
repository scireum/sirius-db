/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.schema;

import sirius.kernel.commons.Strings;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Provides a base implementation of {@link DatabaseDialect}.
 */
public abstract class BasicDatabaseDialect implements DatabaseDialect {

    @Override
    public int getJDBCType(Class<?> clazz) {
        return resolveType(clazz);
    }

    protected int resolveType(Class<?> clazz) {
        if (String.class.equals(clazz)) {
            return Types.VARCHAR;
        }
        if (Integer.class.equals(clazz)) {
            return Types.INTEGER;
        }
        if (Long.class.equals(clazz)) {
            return Types.BIGINT;
        }
        if (Double.class.equals(clazz)) {
            return Types.DOUBLE;
        }
        if (BigDecimal.class.equals(clazz)) {
            return Types.DECIMAL;
        }
        if (Float.class.equals(clazz)) {
            return Types.FLOAT;
        }
        if (Boolean.class.equals(clazz)) {
            return Types.TINYINT;
        }

        return resolveTemporalType(clazz);
    }

    private int resolveTemporalType(Class<?> clazz) {
        if (Date.class.equals(clazz)) {
            return Types.DATE;
        }
        if (Time.class.equals(clazz)) {
            return Types.TIME;
        }
        if (Timestamp.class.equals(clazz)) {
            return Types.TIMESTAMP;
        }

        return resolveBinaryType(clazz);
    }

    private int resolveBinaryType(Class<?> clazz) {
        if (Clob.class.equals(clazz)) {
            return Types.CLOB;
        }
        if (Blob.class.equals(clazz)) {
            return Types.BLOB;
        }

        return resolvePrimitiveType(clazz);
    }

    private int resolvePrimitiveType(Class<?> clazz) {
        if (int.class.equals(clazz)) {
            return Types.INTEGER;
        }
        if (long.class.equals(clazz)) {
            return Types.BIGINT;
        }
        if (double.class.equals(clazz)) {
            return Types.DOUBLE;
        }
        if (float.class.equals(clazz)) {
            return Types.FLOAT;
        }
        if (boolean.class.equals(clazz)) {
            return Types.TINYINT;
        }

        throw new IllegalArgumentException(Strings.apply("The class $s cannot be converted into a JDBC type!", clazz));
    }
}
