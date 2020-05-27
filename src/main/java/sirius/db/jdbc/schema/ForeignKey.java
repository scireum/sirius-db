/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.kernel.commons.ComparableTuple;
import sirius.kernel.commons.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a foreign key.
 */
public class ForeignKey {
    private String name;
    private List<ComparableTuple<Integer, String>> keyFields = new ArrayList<>();
    private List<ComparableTuple<Integer, String>> foreignKeyFields = new ArrayList<>();
    private String foreignTable;

    /**
     * Returns the name of the foreign key.
     *
     * @return the name of the foreign key
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the foreign key.
     *
     * @param name then name of the foreign key
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the name of the referenced table.
     *
     * @return the name of the referenced table
     */
    public String getForeignTable() {
        return foreignTable;
    }

    /**
     * Sets the name of the referenced table.
     *
     * @param foreignTable the name of the referenced table
     */
    public void setForeignTable(String foreignTable) {
        this.foreignTable = foreignTable;
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

    /**
     * Returns the columns matched in the referenced table.
     *
     * @return the columns matched in the referenced table
     */
    public List<String> getForeignColumns() {
        List<String> columns = new ArrayList<>();
        for (ComparableTuple<Integer, String> field : foreignKeyFields) {
            columns.add(field.getSecond());
        }
        return columns;
    }

    @Override
    public String toString() {
        return name + "(" + Strings.join(getColumns(), ", ") + ") -> " + foreignTable + " (" + Strings.join(
                getForeignColumns(),
                ", ") + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ForeignKey)) {
            return false;
        }

        return ((ForeignKey) obj).name.equalsIgnoreCase(name);
    }

    @Override
    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    /**
     * Adds a column to the key.
     *
     * @param pos   the position to add at
     * @param field the field or column to add
     */
    public void addColumn(int pos, String field) {
        keyFields.add(ComparableTuple.create(pos, field));
        Collections.sort(keyFields);
    }

    /**
     * Adds a foreign column which has to be matched be the local ones.
     *
     * @param pos   the position to add at
     * @param field the field or column to add
     */
    public void addForeignColumn(int pos, String field) {
        foreignKeyFields.add(ComparableTuple.create(pos, field));
        Collections.sort(foreignKeyFields);
    }
}
