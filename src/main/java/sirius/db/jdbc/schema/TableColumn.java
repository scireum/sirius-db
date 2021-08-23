/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.db.mixing.Property;
import sirius.db.mixing.annotations.Numeric;
import sirius.kernel.commons.Strings;

import javax.annotation.Nullable;

/**
 * Represents a column of a database table.
 */
public class TableColumn {
    private String oldName;
    private String name;
    private boolean autoIncrement;
    private int type;
    private boolean nullable;
    private int length;
    private int precision;
    private int scale;
    private String defaultValue;
    private Property source;

    /**
     * Creates a new table column.
     */
    public TableColumn() {
    }

    /**
     * Creates a new table column which is pre-initialized with the given property and type.
     *
     * @param property the property used to determine common parameters (nullability, length, default value ..)
     * @param sqlType  the type of the column based on {@link java.sql.Types}
     */
    public TableColumn(Property property, int sqlType) {
        this.source = property;
        this.name = property.getPropertyName();
        this.nullable = property.isNullable();
        this.length = property.getLength();
        this.defaultValue = property.getColumnDefaultValue();
        this.type = sqlType;

        property.getAnnotation(Numeric.class).ifPresent(numeric -> {
            this.precision = numeric.precision();
            this.scale = numeric.scale();
        });
    }

    /**
     * Returns the underlying property if available
     *
     * @return the underlying property
     */
    @Nullable
    public Property getSource() {
        return source;
    }

    /**
     * Determines whether auto increment is enabled for this column.
     *
     * @return <tt>true</tt> if this is an auto increment column, <tt>false</tt> otherwise.
     */
    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    /**
     * Sets the auto increment flag of this column
     *
     * @param autoIncrement <tt>true</tt> if this is an auto increment column, <tt>false</tt> otherwise.
     */
    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    /**
     * Returns the old name of the column.
     *
     * @return the old name of the column or <tt>null</tt> if the column was not renamed
     */
    public String getOldName() {
        return oldName;
    }

    /**
     * Set the old name of a column. This can be used to rename instead of
     * DROP/ADD columns.
     *
     * @param oldName the previous name of the column
     */
    public void setOldName(String oldName) {
        this.oldName = oldName;
    }

    /**
     * Returns the name of the column.
     *
     * @return the name of the column
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the column.
     *
     * @param name the name of the column
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the JDBC type of the column.
     *
     * @return the JDBC type of the column
     * @see java.sql.Types
     */
    public int getType() {
        return type;
    }

    /**
     * Sets the JDBC type of the column.
     *
     * @param type the type to set
     * @see java.sql.Types
     */
    public void setType(int type) {
        this.type = type;
    }

    /**
     * Determines the nullability of the column.
     *
     * @return <tt>true</tt> if the column may be <tt>null</tt>, <tt>false</tt> otherwise
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Specifies the nullability of the column.
     *
     * @param nullable <tt>true</tt> if the column may be <tt>null</tt>, <tt>false</tt> otherwise
     */
    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    /**
     * Returns the overall max. length of the column.
     *
     * @return the max length of the column
     */
    public int getLength() {
        return length;
    }

    /**
     * Specifies the max. length of the column.
     *
     * @param length the max length of this column
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * Returns the precision of the column.
     *
     * @return the total number of digits that can be stored without a rounding errors.
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Specifies a precision of the column.
     *
     * @param precision the total number of digits which can be stored without rounding errors.
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * Returns the number decimal places after the comma.
     *
     * @return the number of decimal places
     */
    public int getScale() {
        return scale;
    }

    /**
     * Specifies the number of decimal places.
     *
     * @param scale the number of decimal places after the comma
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * Returns the default value as string.
     *
     * @return the default value of this column
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Specifies the default value.
     *
     * @param defaultValue the default value as string
     */
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TableColumn)) {
            return false;
        }
        return Strings.areEqual(((TableColumn) obj).name, name);
    }

    @Override
    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }
}
