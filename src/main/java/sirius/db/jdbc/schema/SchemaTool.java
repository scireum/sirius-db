/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.db.jdbc.Database;
import sirius.kernel.commons.ComparableTuple;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Helper class for manipulating and inspecting db metadata.
 */
public class SchemaTool {

    private static final String COLUMN_KEY_SEQ = "KEY_SEQ";
    private static final String COLUMN_COLUMN_NAME = "COLUMN_NAME";
    private static final String KEY_TABLE = "table";
    private static final String KEY_OLD_NAME = "oldName";
    private static final String KEY_COLUMN = "column";
    private final String realm;
    private final DatabaseDialect dialect;

    private static Map<Integer, String> sqlTypeToName;

    /**
     * Creates a new instance for the given dialect.
     *
     * @param realm   the realm this schema is for
     * @param dialect the dialect to use when generating schema change actions.
     */
    public SchemaTool(String realm, DatabaseDialect dialect) {
        this.realm = realm;
        this.dialect = dialect;
    }

    /**
     * Reads the DB-schema for the given connection.
     *
     * @param db the database to read the schema from
     * @return the determined schema
     * @throws SQLException in case of a database error
     */
    public List<Table> getSchema(Database db) throws SQLException {
        List<Table> tables = new ArrayList<>();
        try (Connection c = db.getConnection()) {
            try (ResultSet rs = c.getMetaData().getTables(c.getCatalog(), c.getSchema(), null, null)) {
                while (rs.next()) {
                    readTableRow(tables, c, rs);
                }
            }
        }
        return tables;
    }

    protected void readTableRow(List<Table> tables, Connection c, ResultSet rs) throws SQLException {
        if (!"TABLE".equalsIgnoreCase(rs.getString(4))) {
            return;
        }

        Table table = new Table();
        table.setName(rs.getString("TABLE_NAME"));
        fillTable(c, table);
        tables.add(dialect.completeTableInfos(table));
    }

    private void fillTable(Connection c, Table table) throws SQLException {
        fillColumns(c, table);
        fillPK(c, table);
        fillIndices(c, table);
        fillFKs(c, table);
    }

    private void fillFKs(Connection c, Table table) throws SQLException {
        ResultSet rs = c.getMetaData().getImportedKeys(c.getCatalog(), c.getSchema(), table.getName());
        while (rs.next()) {
            String indexName = rs.getString("FK_NAME");
            if (indexName != null) {
                ForeignKey fk = table.getForeignKey(indexName);
                if (fk == null) {
                    fk = new ForeignKey();
                    fk.setName(indexName);
                    fk.setForeignTable(rs.getString("PKTABLE_NAME"));
                    table.getForeignKeys().add(fk);
                }
                fk.addColumn(rs.getInt(COLUMN_KEY_SEQ), rs.getString("FKCOLUMN_NAME"));
                fk.addForeignColumn(rs.getInt(COLUMN_KEY_SEQ), rs.getString("PKCOLUMN_NAME"));
            }
        }
        rs.close();
    }

    private void fillIndices(Connection c, Table table) throws SQLException {
        ResultSet rs = c.getMetaData().getIndexInfo(c.getCatalog(), c.getSchema(), table.getName(), false, false);
        while (rs.next()) {
            String indexName = rs.getString("INDEX_NAME");
            if (indexName != null) {
                Key key = table.getKey(indexName);
                if (key == null) {
                    key = new Key();
                    key.setName(indexName);
                    key.setUnique(!rs.getBoolean("NON_UNIQUE"));
                    table.getKeys().add(key);
                }
                key.addColumn(rs.getInt("ORDINAL_POSITION"), rs.getString(COLUMN_COLUMN_NAME));
            }
        }
        rs.close();
    }

    private void fillPK(Connection c, Table table) throws SQLException {
        ResultSet rs = c.getMetaData().getPrimaryKeys(c.getCatalog(), c.getSchema(), table.getName());
        List<ComparableTuple<Integer, String>> keyFields = new ArrayList<>();
        while (rs.next()) {
            keyFields.add(ComparableTuple.create(rs.getInt(COLUMN_KEY_SEQ), rs.getString(COLUMN_COLUMN_NAME)));
        }
        Collections.sort(keyFields);
        for (Tuple<Integer, String> key : keyFields) {
            table.getPrimaryKey().add(key.getSecond());
        }
        rs.close();
    }

    private void fillColumns(Connection c, Table table) throws SQLException {
        ResultSet rs = c.getMetaData().getColumns(c.getCatalog(), c.getSchema(), table.getName(), null);
        while (rs.next()) {
            TableColumn column = new TableColumn();
            column.setName(rs.getString(COLUMN_COLUMN_NAME));
            column.setNullable(DatabaseMetaData.columnNullable == rs.getInt("NULLABLE"));
            column.setType(rs.getInt("DATA_TYPE"));
            column.setLength(rs.getInt("COLUMN_SIZE"));
            column.setPrecision(rs.getInt("COLUMN_SIZE"));
            column.setScale(rs.getInt("DECIMAL_DIGITS"));
            column.setDefaultValue(dialect.getDefaultValue(rs));
            table.getColumns().add(column);
        }
        rs.close();
    }

    /**
     * Generates a list of schema change actions.
     * <p>
     * Compares the expected target schema to the database connected via the given connection.
     *
     * @param db           the database used to determine the existing schema
     * @param targetSchema the target schema defined by {@link Schema}
     * @param dropTables   determines if unknown tables should be dropped or not
     * @return a list of change actions to that the existing schema matches the expected one
     * @throws SQLException in case of a database error
     */
    public List<SchemaUpdateAction> migrateSchemaTo(Database db, List<Table> targetSchema, boolean dropTables)
            throws SQLException {
        List<SchemaUpdateAction> result = new ArrayList<>();
        List<Table> currentSchema = getSchema(db);

        syncRequiredTables(result, currentSchema, targetSchema);
        if (dropTables) {
            dropUnusedTables(result, currentSchema, targetSchema);
        }

        return result;
    }

    private void dropUnusedTables(List<SchemaUpdateAction> result,
                                  List<Table> currentSchema,
                                  List<Table> targetSchema) {
        for (Table table : currentSchema) {
            if (!isTableInUse(targetSchema, table)) {
                String sql = dialect.generateDropTable(table);
                if (Strings.isFilled(sql)) {
                    SchemaUpdateAction action = new SchemaUpdateAction(realm);
                    action.setReason(NLS.fmtr("SchemaTool.tableUnused").set(KEY_TABLE, table.getName()).format());
                    action.setDataLossPossible(true);
                    action.setSql(sql);
                    result.add(action);
                }
            }
        }
    }

    private boolean isTableInUse(List<Table> targetSchema, Table currentTable) {
        Table result = findInList(targetSchema, currentTable);
        if (result != null) {
            return true;
        }
        return targetSchema.stream()
                           .anyMatch(targetTable -> Strings.areEqual(currentTable.getName(), targetTable.getOldName()));
    }

    @SuppressWarnings("squid:S3047")
    @Explain("False positive - targetSchema is modified in the first loop.")
    private void syncRequiredTables(List<SchemaUpdateAction> result,
                                    List<Table> currentSchema,
                                    List<Table> targetSchema) {
        for (Table targetTable : targetSchema) {
            generateEffectiveKeyNames(targetTable);

            Table other = findTable(currentSchema, targetTable);
            if (other == null) {
                createTable(targetTable, result);
            } else {
                if (!Strings.areEqual(targetTable.getName(), other.getName())) {
                    renameTable(targetTable, result);
                }
                syncTables(targetTable, other, result);
            }
        }

        for (Table targetTable : targetSchema) {
            Table other = findTable(currentSchema, targetTable);
            syncForeignKeys(targetTable, other, result);
        }
    }

    private Table findTable(List<Table> currentSchema, Table targetTable) {
        Table result = findInList(currentSchema, targetTable);
        if (result != null) {
            return result;
        }
        if (Strings.isEmpty(targetTable.getOldName())) {
            return null;
        }
        return currentSchema.stream()
                            .filter(table -> Strings.areEqual(table.getName(), targetTable.getOldName()))
                            .findAny()
                            .orElse(null);
    }

    private void createTable(Table targetTable, List<SchemaUpdateAction> result) {
        String sql = dialect.generateCreateTable(targetTable);
        if (Strings.isFilled(sql)) {
            SchemaUpdateAction action = new SchemaUpdateAction(realm);
            action.setReason(NLS.fmtr("SchemaTool.tableDoesNotExist").set(KEY_TABLE, targetTable.getName()).format());
            action.setDataLossPossible(false);
            action.setSql(sql);
            result.add(action);
        }
    }

    private void renameTable(Table table, List<SchemaUpdateAction> result) {
        String sql = dialect.generateRenameTable(table);
        if (Strings.isFilled(sql)) {
            SchemaUpdateAction action = new SchemaUpdateAction(realm);
            action.setReason(NLS.fmtr("SchemaTool.tableNeedsRename")
                                .set(KEY_TABLE, table.getName())
                                .set(KEY_OLD_NAME, table.getOldName())
                                .format());
            action.setDataLossPossible(false);
            action.setSql(sql);
            result.add(action);
        }
    }

    private boolean keyListEqual(List<String> left, List<String> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!left.get(i).equalsIgnoreCase(right.get(i))) {
                return false;
            }
        }
        return true;
    }

    private void syncTables(Table targetTable, Table other, List<SchemaUpdateAction> result) {
        dropForeignKeys(targetTable, other, result);
        dropKeys(targetTable, other, result);
        syncColumns(targetTable, other, result);
        syncKeys(targetTable, other, result);
        if (!keyListEqual(targetTable.getPrimaryKey(), other.getPrimaryKey())) {
            List<String> sql = dialect.generateAlterPrimaryKey(targetTable);
            if (!sql.isEmpty()) {
                SchemaUpdateAction action = new SchemaUpdateAction(realm);
                action.setReason(NLS.fmtr("SchemaTool.pkChanged").set(KEY_TABLE, targetTable.getName()).format());
                action.setDataLossPossible(true);
                action.setSql(sql);
                result.add(action);
            }
        }
    }

    private void generateEffectiveKeyNames(Table targetTable) {
        targetTable.getKeys().forEach(key -> key.setName(dialect.getEffectiveKeyName(targetTable, key)));
    }

    private void dropKeys(Table targetTable, Table other, List<SchemaUpdateAction> result) {
        for (Key key : other.getKeys()) {
            ForeignKey fk = new ForeignKey();
            fk.setName(key.getName());
            if (findInList(targetTable.getKeys(), key) == null
                && findInList(targetTable.getForeignKeys(), fk) == null
                && dialect.shouldDropKey(targetTable, other, key)) {
                String sql = dialect.generateDropKey(other, key);
                if (Strings.isFilled(sql)) {
                    SchemaUpdateAction action = new SchemaUpdateAction(realm);
                    action.setReason(NLS.fmtr("SchemaTool.indexUnused")
                                        .set("key", key.getName())
                                        .set(KEY_TABLE, other.getName())
                                        .format());
                    action.setDataLossPossible(true);
                    action.setSql(sql);
                    result.add(action);
                }
            }
        }
    }

    private void dropForeignKeys(Table targetTable, Table other, List<SchemaUpdateAction> result) {
        for (ForeignKey key : other.getForeignKeys()) {
            if (findInList(targetTable.getForeignKeys(), key) == null) {
                String sql = dialect.generateDropForeignKey(targetTable, key);
                if (Strings.isFilled(sql)) {
                    SchemaUpdateAction action = new SchemaUpdateAction(realm);
                    action.setReason(NLS.fmtr("SchemaTool.fkUnused")
                                        .set("key", key.getName())
                                        .set(KEY_TABLE, other.getName())
                                        .format());
                    action.setDataLossPossible(true);
                    action.setSql(sql);
                    result.add(action);
                }
            }
        }
    }

    private void syncKeys(Table targetTable, Table other, List<SchemaUpdateAction> result) {
        for (Key targetKey : targetTable.getKeys()) {
            Key otherKey = findInList(other.getKeys(), targetKey);
            if (otherKey == null) {
                createKey(targetTable, result, targetKey);
            } else {
                adjustKey(targetTable, result, targetKey, otherKey);
            }
        }
    }

    private void createKey(Table table, List<SchemaUpdateAction> result, Key targetKey) {
        String sql = dialect.generateAddKey(table, targetKey);
        if (Strings.isFilled(sql)) {
            SchemaUpdateAction action = new SchemaUpdateAction(realm);
            action.setReason(NLS.fmtr("SchemaTool.indexDoesNotExist")
                                .set("key", targetKey.getName())
                                .set(KEY_TABLE, table.getName())
                                .format());
            action.setDataLossPossible(false);
            action.setSql(sql);
            result.add(action);
        }
    }

    private void adjustKey(Table table, List<SchemaUpdateAction> result, Key targetKey, Key otherKey) {
        if (!keyListEqual(targetKey.getColumns(), otherKey.getColumns())
            || targetKey.isUnique() != otherKey.isUnique()) {
            List<String> sql = dialect.generateAlterKey(table, otherKey, targetKey);
            if (!sql.isEmpty()) {
                SchemaUpdateAction action = new SchemaUpdateAction(realm);
                action.setReason(NLS.fmtr("SchemaTool.indexNeedsChange")
                                    .set("key", targetKey.getName())
                                    .set(KEY_TABLE, table.getName())
                                    .format());
                action.setDataLossPossible(true);
                action.setSql(sql);
                result.add(action);
            }
        }
    }

    private void syncForeignKeys(Table targetTable, Table other, List<SchemaUpdateAction> result) {
        boolean isNewTable = (other == null);
        for (ForeignKey targetKey : targetTable.getForeignKeys()) {
            ForeignKey otherKey = isNewTable ? null : findInList(other.getForeignKeys(), targetKey);
            if (otherKey == null) {
                createForeignKey(targetTable, result, targetKey, isNewTable);
            } else {
                adjustForeignKey(targetTable, result, targetKey, otherKey);
            }
        }
    }

    private void createForeignKey(Table targetTable,
                                  List<SchemaUpdateAction> result,
                                  ForeignKey targetKey,
                                  boolean isNewTable) {
        String sql = dialect.generateAddForeignKey(targetTable, targetKey);
        if (Strings.isFilled(sql)) {
            SchemaUpdateAction action = new SchemaUpdateAction(realm);
            action.setReason(NLS.fmtr("SchemaTool.fkDoesNotExist")
                                .set("key", targetKey.getName())
                                .set(KEY_TABLE, targetTable.getName())
                                .format());
            action.setDataLossPossible(!isNewTable);
            action.setSql(sql);
            result.add(action);
        }
    }

    private void adjustForeignKey(Table targetTable,
                                  List<SchemaUpdateAction> result,
                                  ForeignKey targetKey,
                                  ForeignKey otherKey) {
        if (!keyListEqual(targetKey.getColumns(), otherKey.getColumns())
            || !keyListEqual(targetKey.getForeignColumns(),
                             otherKey.getForeignColumns())
            || !targetKey.getForeignTable().equalsIgnoreCase(otherKey.getForeignTable())) {
            List<String> sql = dialect.generateAlterForeignKey(targetTable, otherKey, targetKey);
            if (!sql.isEmpty()) {
                SchemaUpdateAction action = new SchemaUpdateAction(realm);
                action.setReason(NLS.fmtr("SchemaTool.fkNeedsChange")
                                    .set("key", targetKey.getName())
                                    .set(KEY_TABLE, targetTable.getName())
                                    .format());
                action.setDataLossPossible(true);
                action.setSql(sql);
                result.add(action);
            }
        }
    }

    private void syncColumns(Table targetTable, Table other, List<SchemaUpdateAction> result) {
        Set<String> usedColumns = new TreeSet<>();
        for (TableColumn targetCol : targetTable.getColumns()) {
            // Try to find column by name
            TableColumn otherCol = findColumn(other, targetCol.getName());
            // If we didn't find a column and the col has rename infos, try to
            // find an appropriate column for renaming.
            if (otherCol == null && targetCol.getOldName() != null) {
                otherCol = findColumn(other, targetCol.getOldName());
            }
            if (otherCol == null) {
                handleNewColumn(targetTable, result, targetCol);
            } else {
                handleUpdateColumn(targetTable, result, usedColumns, targetCol, otherCol);
            }
        }

        handleUnusedColumns(targetTable, other, result, usedColumns);
    }

    private void handleUnusedColumns(Table targetTable,
                                     Table other,
                                     List<SchemaUpdateAction> result,
                                     Set<String> usedColumns) {
        for (TableColumn col : other.getColumns()) {
            if (!usedColumns.contains(col.getName())) {
                String sql = dialect.generateDropColumn(targetTable, col);
                if (Strings.isFilled(sql)) {
                    SchemaUpdateAction action = new SchemaUpdateAction(realm);
                    action.setReason(NLS.fmtr("SchemaTool.columnUnused")
                                        .set(KEY_COLUMN, col.getName())
                                        .set(KEY_TABLE, other.getName())
                                        .format());
                    action.setDataLossPossible(true);
                    action.setSql(sql);
                    result.add(action);
                }
            }
        }
    }

    private void handleUpdateColumn(Table table,
                                    List<SchemaUpdateAction> result,
                                    Set<String> usedColumns,
                                    TableColumn targetCol,
                                    TableColumn otherCol) {
        usedColumns.add(otherCol.getName());
        String reason = dialect.areColumnsEqual(targetCol, otherCol);
        boolean dataLossPossible = true;
        // Check for renaming...
        if (reason == null
            && !Strings.areEqual(targetCol.getName(), otherCol.getName())
            && dialect.isColumnCaseSensitive()) {
            reason = NLS.fmtr("SchemaTool.columnNeedsRename")
                        .set(KEY_COLUMN, otherCol.getName())
                        .set("newName", targetCol.getName())
                        .set(KEY_TABLE, table.getName())
                        .format();
            dataLossPossible = false;
        } else if (reason != null) {
            reason = NLS.fmtr("SchemaTool.columnNeedsChange")
                        .set(KEY_COLUMN, otherCol.getName())
                        .set(KEY_TABLE, table.getName())
                        .set("reason", reason)
                        .format();
        }
        if (reason != null) {
            List<String> sql = dialect.generateAlterColumnTo(table, otherCol.getName(), targetCol);
            if (!sql.isEmpty()) {
                SchemaUpdateAction action = new SchemaUpdateAction(realm);
                action.setReason(reason);
                action.setDataLossPossible(dataLossPossible);
                action.setSql(sql);
                result.add(action);
            }
        }
    }

    private void handleNewColumn(Table table, List<SchemaUpdateAction> result, TableColumn targetCol) {
        String sql = dialect.generateAddColumn(table, targetCol);
        if (Strings.isFilled(sql)) {
            SchemaUpdateAction action = new SchemaUpdateAction(realm);
            action.setReason(NLS.fmtr("SchemaTool.columnDoesNotExist")
                                .set(KEY_COLUMN, targetCol.getName())
                                .set(KEY_TABLE, table.getName())
                                .format());
            action.setDataLossPossible(false);
            action.setSql(sql);
            result.add(action);
        }
    }

    private TableColumn findColumn(Table other, String name) {
        String effectiveName = dialect.translateColumnName(name);
        for (TableColumn col : other.getColumns()) {
            if (Strings.areEqual(col.getName(), effectiveName)) {
                return col;
            }
        }
        return null;
    }

    private <X> X findInList(List<X> haystack, X needle) {
        return haystack.stream().filter(item -> Objects.equals(item, needle)).findAny().orElse(null);
    }

    /**
     * Generates a type name for the given int constant for a JDBC type defined in {@link java.sql.Types}.
     *
     * @param jdbcType the type to convert
     * @return the string representation of the given type
     */
    public static String getJdbcTypeName(int jdbcType) {
        if (sqlTypeToName == null) {
            Map<Integer, String> result = new HashMap<>();
            Arrays.stream(Types.class.getFields()).forEach(field -> {
                try {
                    result.put((Integer) field.get(null), field.getName());
                } catch (IllegalAccessException e) {
                    Exceptions.ignore(e);
                }
            });
            sqlTypeToName = result;
        }

        return sqlTypeToName.get(jdbcType);
    }
}
