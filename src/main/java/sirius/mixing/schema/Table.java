package sirius.mixing.schema;

import sirius.kernel.commons.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a database table.
 */
public class Table {
    private String name;
    private List<String> primaryKey = new ArrayList<String>();
    private List<TableColumn> columns = new ArrayList<TableColumn>();
    private List<Key> keys = new ArrayList<Key>();
    private List<ForeignKey> foreignKeys = new ArrayList<ForeignKey>();

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public List<Key> getKeys() {
        return keys;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public Key getKey(String indexName) {
        for (Key key : keys) {
            if (Strings.areEqual(indexName, key.getName())) {
                return key;
            }
        }
        return null;
    }

    public ForeignKey getFK(String indexName) {
        for (ForeignKey key : foreignKeys) {
            if (Strings.areEqual(indexName, key.getName())) {
                return key;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return name;
    }

    public String dump() {
        StringBuilder sb = new StringBuilder("TABLE ");
        sb.append(name);
        sb.append("{\n");
        for (TableColumn col : columns) {
            sb.append("   COLUMN: ");
            sb.append(col);
            sb.append("\n");
        }
        sb.append("   PK: ");
        sb.append(Strings.join(primaryKey, ", "));
        sb.append("\n");
        for (Key key : keys) {
            sb.append("   INDEX: ");
            sb.append(key);
            sb.append("\n");
        }
        for (ForeignKey fk : foreignKeys) {
            sb.append("   FK: ");
            sb.append(fk);
            sb.append("\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Table)) {
            return false;
        }
        return ((Table) obj).name.equalsIgnoreCase(name);
    }

    @Override
    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }
}
