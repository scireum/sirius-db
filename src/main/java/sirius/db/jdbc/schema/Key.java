/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import com.google.common.collect.Lists;
import sirius.kernel.commons.ComparableTuple;
import sirius.kernel.commons.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a database key or index.
 */
public class Key {

    private boolean unique;
    private String name;
    private List<ComparableTuple<Integer, String>> keyFields = Lists.newArrayList();

    /**
     * Returns the name of the key.
     *
     * @return the name of the key
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the key.
     *
     * @param name then name of the key
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the columns that make up the key.
     *
     * @return the columns of the key
     */
    public List<String> getColumns() {
        List<String> columns = new ArrayList<>();
        for (ComparableTuple<Integer, String> field : keyFields) {
            columns.add(field.getSecond());
        }
        return columns;
    }

    @Override
    public String toString() {
        return name + " (" + Strings.join(getColumns(), ", ") + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Key)) {
            return false;
        }
        return ((Key) obj).name.equalsIgnoreCase(name);
    }

    @Override
    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    /**
     * Adds a column to the key
     *
     * @param pos   the position to add at
     * @param field the field or column to add
     */
    public void addColumn(int pos, String field) {
        keyFields.add(ComparableTuple.create(pos, field));
        Collections.sort(keyFields);
    }

    /**
     * Determines if this key also enforces an unique constraint
     *
     * @return <tt>true</tt> if the values in the indexed columns have to be unique, <tt>false</tt> otherwise
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Sets the unique flag.
     *
     * @param unique <tt>true</tt> to enforce an unique constraint, <tt>false</tt> otherwise
     */
    public void setUnique(boolean unique) {
        this.unique = unique;
    }
}
