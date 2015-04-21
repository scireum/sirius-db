package sirius.mixing.schema;

import java.util.Collections;
import java.util.List;

/**
 * Represents an action which needs to be perfomed onto a schema to make it
 * match another schema.
 *
 * @author aha
 */
public class SchemaUpdateAction {
    private String reason;
    private List<String> sql;
    private boolean dataLossPossible;

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public List<String> getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = Collections.singletonList(sql);
    }

    public void setSql(List<String> sql) {
        this.sql = sql;
    }

    public boolean isDataLossPossible() {
        return dataLossPossible;
    }

    public void setDataLossPossible(boolean dataLossPossible) {
        this.dataLossPossible = dataLossPossible;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getReason());
        for (String statement : sql) {
            sb.append("\n");
            sb.append(statement);
        }

        return sb.toString();
    }
}
