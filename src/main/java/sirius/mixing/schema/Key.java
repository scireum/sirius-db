package sirius.mixing.schema;

import sirius.kernel.commons.ComparableTuple;
import sirius.kernel.commons.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a database key or index.
 */
public class Key {
    private String name;
    private List<ComparableTuple<Integer, String>> keyFields = new ArrayList<ComparableTuple<Integer, String>>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getColumns() {
        List<String> columns = new ArrayList<String>();
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

    public void addColumn(int pos, String field) {
        keyFields.add(ComparableTuple.create(pos, field));
        Collections.sort(keyFields);
    }
}
