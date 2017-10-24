/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import sirius.db.jdbc.Capability;
import sirius.db.jdbc.Database;
import sirius.db.jdbc.SQLQuery;
import sirius.db.mixing.constraints.FieldOperator;
import sirius.db.mixing.properties.EntityRefProperty;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Microtiming;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Provides a query DSL which is used to query {@link Entity} instances from the database.
 *
 * @param <E> the generic type of entities being queried
 */
public class SmartQuery<E extends Entity> extends BaseQuery<E> {

    @Part
    private static OMA oma;

    protected final EntityDescriptor descriptor;
    protected List<Column> fields = Collections.emptyList();
    protected boolean distinct;
    protected List<Tuple<Column, Boolean>> orderBys = Lists.newArrayList();
    protected List<Constraint> constaints = Lists.newArrayList();
    protected Database db;

    /**
     * Creates a new query instance.
     * <p>
     * Use {@link OMA#select(Class)} to create a new query.
     *
     * @param type the entity type to select
     * @param db   the database to operate on
     */
    protected SmartQuery(Class<E> type, Database db) {
        super(type);
        this.db = db;
        this.descriptor = getDescriptor();
    }

    /**
     * Returns the <tt>EntityDescriptor</tt> for the type of entities addressed by this query.
     *
     * @return the underlying entity descriptor of this query.
     */
    public EntityDescriptor getEntityDescriptor() {
        return descriptor;
    }

    @Override
    public SmartQuery<E> skip(int skip) {
        return (SmartQuery<E>) super.skip(skip);
    }

    @Override
    public SmartQuery<E> limit(int limit) {
        return (SmartQuery<E>) super.limit(limit);
    }

    /**
     * Applies the given contraints to the query.
     *
     * @param constraints the constraints which have to be fullfilled
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> where(Constraint... constraints) {
        Collections.addAll(this.constaints, constraints);

        return this;
    }

    /**
     * Adds an {@link FieldOperator#eq(Object)} constraint for the given field and value.
     *
     * @param field the field to check
     * @param value the value to filter on
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> eq(Column field, Object value) {
        this.constaints.add(FieldOperator.on(field).eq(value));
        return this;
    }

    /**
     * Adds an {@link FieldOperator#eq(Object)} constraint for the given field and value, if the value is non-null.
     * <p>
     * If the given value is <tt>null</tt>, the constraint is skipped.
     *
     * @param field the field to check
     * @param value the value to filter on
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> eqIgnoreNull(Column field, Object value) {
        this.constaints.add(FieldOperator.on(field).eq(value).ignoreNull());
        return this;
    }

    /**
     * Adds an ascending order on the given field.
     *
     * @param field the field to order by
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> orderAsc(Column field) {
        orderBys.add(Tuple.create(field, true));
        return this;
    }

    /**
     * Adds a descending order on the given field.
     *
     * @param field the field to order by
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> orderDesc(Column field) {
        orderBys.add(Tuple.create(field, false));
        return this;
    }

    /**
     * Specifies the fields to select, which also have to be <tt>DISTINCT</tt>.
     *
     * @param fields the fields to select and to apply a <tt>DISTINCT</tt> filter on.
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> distinctFields(Column... fields) {
        this.fields = Arrays.asList(fields);
        this.distinct = true;
        return this;
    }

    /**
     * Specifies which fields to select.
     * <p>
     * If no fields are given, <tt>*</tt> is selected
     *
     * @param fields the list of fields to select
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> fields(Column... fields) {
        this.fields = new ArrayList<>(Arrays.asList(fields));
        checkJoinColumnsPresent();

        return this;
    }

    /**
     * Aggregates all columns which must be present to fetch all join-columns so that the {@link EntityRef}
     * implementation works correctly.
     *
     * @return a map of columns which need to be present
     */
    private HashMap<String, Column> aggregateRequiredJoinColumns() {
        HashMap<String, Column> requiredJoinFields = new HashMap<>();

        this.fields.forEach(field -> {
            Monoflop mf = Monoflop.create();
            StringBuilder fieldName = new StringBuilder();
            Column currentField = field.getParent();

            while (currentField != null) {
                if (mf.successiveCall()) {
                    fieldName.append(".");
                }

                fieldName.append(currentField.getName());

                if (!requiredJoinFields.containsKey(fieldName.toString())) {
                    requiredJoinFields.put(fieldName.toString(), currentField);
                }

                currentField = currentField.getParent();
            }
        });

        return requiredJoinFields;
    }

    /**
     * Aggregates all columns which where explicitly set and should be fetched using {@link #fields(Column...)}.
     *
     * @return a set of columns which are about to be fetched
     */
    private Set<String> aggregatePresentColumns() {
        Set<String> presentColumnPaths = new HashSet<>();

        this.fields.forEach(field -> {
            Monoflop mf = Monoflop.create();
            StringBuilder fieldName = new StringBuilder();
            Column currentField = field;

            while (currentField != null) {
                if (mf.successiveCall()) {
                    fieldName.append(".");
                }
                fieldName.append(currentField.getName());
                currentField = currentField.getParent();
            }

            presentColumnPaths.add(fieldName.toString());
        });

        return presentColumnPaths;
    }

    /**
     * Synchonizes the columns which where explicitly set using {@link #fields(Column...)} with the columns that are
     * required for join-fetches. If columns are missing, they are automatically added.
     */
    private void checkJoinColumnsPresent() {
        Set<String> presentColumnPaths = aggregatePresentColumns();

        aggregateRequiredJoinColumns().forEach((path, column) -> {
            if (!presentColumnPaths.contains(path)) {
                this.fields.add(column);
            }
        });
    }

    /**
     * Executes the query and counts the number of results.
     *
     * @return the number of matched rows
     */
    public long count() {
        Watch w = Watch.start();
        Compiler compiler = compileCOUNT();
        try {
            try (Connection c = db.getConnection()) {
                return execCount(compiler, c);
            } finally {
                if (Microtiming.isEnabled()) {
                    w.submitMicroTiming("OMA", compiler.toString());
                }
            }
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Error executing query '%s' for type '%s': %s (%s)",
                                                    compiler,
                                                    type.getName())
                            .handle();
        }
    }

    protected long execCount(Compiler compiler, Connection c) throws SQLException {
        try (PreparedStatement stmt = compiler.prepareStatement(c)) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    return 0;
                }
            }
        }
    }

    /**
     * Determines if the query would have at least one matching row.
     *
     * @return <tt>true</tt> if at least one row matches the query, <tt>false</tt> otherwise.
     */
    public boolean exists() {
        return count() > 0;
    }

    /**
     * Deletes all entities matching this query.
     * <p>
     * Note that this will not generate a <tt>DELETE</tt> statement but rather select the results and invoke
     * {@link OMA#delete(Entity)} on each entity to ensure that framework checks are triggered.
     */
    public void delete() {
        iterateAll(oma::delete);
    }

    protected static Set<String> readColumns(ResultSet rs) throws SQLException {
        Set<String> result = Sets.newHashSet();
        for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
            result.add(rs.getMetaData().getColumnLabel(col).toUpperCase());
        }

        return result;
    }

    /**
     * Converts this query into a plain {@link SQLQuery} which will return rows instead of entities.
     *
     * @return the query converted into a plain SQL query.
     */
    public SQLQuery asSQLQuery() {
        Compiler compiler = compileSELECT();
        return new SQLQuery(db, compiler.getQuery()) {
            @Override
            protected PreparedStatement createPreparedStatement(Connection c) throws SQLException {
                return compiler.prepareStatement(c);
            }
        };
    }

    /**
     * Creates a full copy of the query which can be modified without modifying this query.
     *
     * @return a copy of this query
     */
    public SmartQuery<E> copy() {
        SmartQuery<E> copy = new SmartQuery<>(type, db);
        copy.distinct = distinct;
        copy.fields = new ArrayList<>(fields);
        copy.orderBys.addAll(orderBys);
        copy.constaints.addAll(constaints);

        return copy;
    }

    @Override
    public void iterate(Function<E, Boolean> handler) {
        Compiler compiler = compileSELECT();
        try {
            Watch w = Watch.start();
            try (Connection c = db.getConnection(); PreparedStatement stmt = compiler.prepareStatement(c)) {
                Limit limit = getLimit();
                boolean nativeLimit = db.hasCapability(Capability.LIMIT);
                tuneStatement(stmt, limit, nativeLimit);
                try (ResultSet rs = stmt.executeQuery()) {
                    execIterate(handler, compiler, limit, nativeLimit, rs);
                }
            } finally {
                if (Microtiming.isEnabled()) {
                    w.submitMicroTiming("OMA", compiler.toString());
                }
            }
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Error executing query '%s' for type '%s': %s (%s)",
                                                    compiler.toString(),
                                                    type.getName())
                            .handle();
        }
    }

    @SuppressWarnings("unchecked")
    protected void execIterate(Function<E, Boolean> handler,
                               Compiler compiler,
                               Limit limit,
                               boolean nativeLimit,
                               ResultSet rs) throws Exception {
        TaskContext tc = TaskContext.get();
        Set<String> columns = readColumns(rs);
        while (rs.next() && tc.isActive()) {
            if (nativeLimit || limit.nextRow()) {
                Entity e = descriptor.readFrom(null, columns, rs);
                compiler.executeJoinFetches(e, columns, rs);
                if (!handler.apply((E) e)) {
                    return;
                }
            }
            if (!nativeLimit && !limit.shouldContinue()) {
                return;
            }
        }
    }

    protected void tuneStatement(PreparedStatement stmt, Limit limit, boolean nativeLimit) throws SQLException {
        if (!nativeLimit && limit.getTotalItems() > 0) {
            stmt.setMaxRows(limit.getTotalItems());
        }
        if (limit.getTotalItems() > 1000 || limit.getTotalItems() <= 0) {
            if (db.hasCapability(Capability.STREAMING)) {
                stmt.setFetchSize(Integer.MIN_VALUE);
            } else {
                stmt.setFetchSize(1000);
            }
        }
    }

    /**
     * Represents the compiler which is used to generate SQL statements based on a {@link SmartQuery}.
     */
    public static class Compiler {

        private static class JoinFetch {
            String tableAlias;
            EntityRefProperty property;
            Map<String, JoinFetch> subFetches = Maps.newTreeMap();
        }

        protected EntityDescriptor ed;
        protected StringBuilder preJoinQuery = new StringBuilder();
        protected StringBuilder joins = new StringBuilder();
        protected StringBuilder postJoinQuery = new StringBuilder();
        protected List<Object> parameters = Lists.newArrayList();
        protected Map<String, Tuple<String, EntityDescriptor>> joinTable = Maps.newTreeMap();
        protected AtomicInteger aliasCounter = new AtomicInteger(1);
        protected String defaultAlias = "e";
        private JoinFetch rootFetch = new JoinFetch();

        /**
         * Creates a new compiler for the given entity descriptor.
         *
         * @param ed the entity descriptor which is used to determine which table to select and how to JOIN other
         *           entities.
         */
        public Compiler(@Nullable EntityDescriptor ed) {
            this.ed = ed;
        }

        /**
         * Provides access to the string builder which generates the SELECT part of the query.
         *
         * @return the string builder representing the SELECT part of the query
         */
        public StringBuilder getSELECTBuilder() {
            return preJoinQuery;
        }

        /**
         * Provides access to the string builder which generates the WHERE part of the query.
         *
         * @return the string builder representing the WHERE part of the query.
         */
        public StringBuilder getWHEREBuilder() {
            return postJoinQuery;
        }

        /**
         * Generates an unique table alias.
         *
         * @return a table alias which is unique within this query
         */
        public String generateTableAlias() {
            return "t" + aliasCounter.getAndIncrement();
        }

        /**
         * Returns the currently active default alias along with the corresponding <tt>EntityDescritpor</tt>
         * <p>
         * The default alias is the one, on which all field relate which do not require a join. It can be changed for
         * inner queries like EXISTS.
         *
         * @return the currently active default alias and entity descriptor as tuple
         */
        public Tuple<String, EntityDescriptor> getDefaultAlias() {
            return Tuple.create(defaultAlias, ed);
        }

        /**
         * Changes the currently active default alias and entity descriptor.
         * <p>
         * Care should be taken to change the alias back once building a sub query is finised.
         *
         * @param defaultAlias         the new default alias
         * @param newDefaultDescriptor the new default entity descriptor matching the table referenced by the given
         *                             alias
         */
        public void setDefaultAlias(String defaultAlias, EntityDescriptor newDefaultDescriptor) {
            this.defaultAlias = defaultAlias;
            this.ed = newDefaultDescriptor;
        }

        private Tuple<String, EntityDescriptor> determineAlias(Column parent) {
            if (parent == null || ed == null) {
                return Tuple.create(defaultAlias, ed);
            }
            String path = parent.toString();
            Tuple<String, EntityDescriptor> result = joinTable.get(path);
            if (result != null) {
                return result;
            }
            Tuple<String, EntityDescriptor> parentAlias = determineAlias(parent.getParent());
            EntityRefProperty refProperty = (EntityRefProperty) parentAlias.getSecond().getProperty(parent.getName());
            EntityDescriptor other = refProperty.getReferencedDescriptor();
            if (joins == null) {
                joins = new StringBuilder();
            }
            String tableAlias = generateTableAlias();
            joins.append(" LEFT JOIN ")
                 .append(other.getTableName())
                 .append(" ")
                 .append(tableAlias)
                 .append(" ON ")
                 .append(tableAlias)
                 .append(".id = ")
                 .append(parentAlias.getFirst())
                 .append(".")
                 .append(parentAlias.getSecond().rewriteColumnName(parent.getName()));
            result = Tuple.create(tableAlias, other);
            joinTable.put(path, result);
            return result;
        }

        /**
         * Translates a column name into an effective name by applying aliases and rewrites.
         *
         * @param column the column to translate
         * @return the translated name which is used in the database
         */
        public String translateColumnName(Column column) {
            Tuple<String, EntityDescriptor> aliasAndDescriptor = determineAlias(column.getParent());
            EntityDescriptor effectiveDescriptor = aliasAndDescriptor.getSecond();
            if (effectiveDescriptor != null) {
                return aliasAndDescriptor.getFirst() + "." + effectiveDescriptor.rewriteColumnName(column.getName());
            } else {
                return aliasAndDescriptor.getFirst() + "." + column.getName();
            }
        }

        private void createJoinFetch(Column field) {
            List<Column> fetchPath = Lists.newArrayList();
            Column parent = field.getParent();
            while (parent != null) {
                fetchPath.add(0, parent);
                parent = parent.getParent();
            }
            JoinFetch jf = rootFetch;
            EntityDescriptor currentDescriptor = ed;
            for (Column col : fetchPath) {
                JoinFetch subFetch = jf.subFetches.get(col.getName());
                if (subFetch == null) {
                    subFetch = new JoinFetch();
                    Tuple<String, EntityDescriptor> parentInfo = determineAlias(col);
                    subFetch.tableAlias = parentInfo.getFirst();
                    subFetch.property = (EntityRefProperty) currentDescriptor.getProperty(col.getName());
                    jf.subFetches.put(col.getName(), subFetch);
                }
                jf = subFetch;
                currentDescriptor = subFetch.property.getReferencedDescriptor();
            }
        }

        protected void executeJoinFetches(Entity entity, Set<String> columns, ResultSet rs) {
            executeJoinFetch(rootFetch, entity, columns, rs);
        }

        private void executeJoinFetch(JoinFetch jf, Entity parent, Set<String> columns, ResultSet rs) {
            try {
                Entity child = parent;
                if (jf.property != null) {
                    child = jf.property.getReferencedDescriptor().readFrom(jf.tableAlias, columns, rs);
                    jf.property.setReferencedEntity(parent, child);
                }
                for (JoinFetch subFetch : jf.subFetches.values()) {
                    executeJoinFetch(subFetch, child, columns, rs);
                }
            } catch (Exception e) {
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .error(e)
                                .withSystemErrorMessage(
                                        "Error while trying to read join fetched values for %s (%s): %s (%s)",
                                        jf.property,
                                        columns)
                                .handle();
            }
        }

        /**
         * Adds a query parameter.
         *
         * @param parameter the parameter to add
         */
        public void addParameter(Object parameter) {
            parameters.add(parameter);
        }

        private PreparedStatement prepareStatement(Connection c) throws SQLException {
            PreparedStatement stmt =
                    c.prepareStatement(getQuery(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i + 1, parameters.get(i));
            }
            return stmt;
        }

        protected String getQuery() {
            return preJoinQuery.toString() + joins + postJoinQuery;
        }

        @Override
        public String toString() {
            if (parameters.isEmpty()) {
                return getQuery();
            } else {
                return getQuery() + " " + parameters;
            }
        }
    }

    private Compiler compileSELECT() {
        Compiler compiler = select();
        from(compiler);
        where(compiler);
        orderBy(compiler);
        limit(compiler);
        return compiler;
    }

    private Compiler compileCOUNT() {
        Compiler compiler = selectCount();
        from(compiler);
        where(compiler);
        return compiler;
    }

    private Compiler select() {
        Compiler c = new Compiler(descriptor);
        c.getSELECTBuilder().append("SELECT ");
        if (fields.isEmpty()) {
            c.getSELECTBuilder().append(" ").append(c.defaultAlias).append(".*");
        } else {
            if (distinct) {
                c.getSELECTBuilder().append("DISTINCT ");
            }
            appendFieldList(c, true);
        }
        return c;
    }

    private void appendFieldList(Compiler c, boolean applyAliases) {
        Monoflop mf = Monoflop.create();
        for (Column field : fields) {
            if (mf.successiveCall()) {
                c.getSELECTBuilder().append(", ");
            }
            Tuple<String, EntityDescriptor> joinInfo = c.determineAlias(field.getParent());
            c.getSELECTBuilder().append(joinInfo.getFirst());
            c.getSELECTBuilder().append(".");
            String columnName = fetchEffectiveColumnName(field, joinInfo);
            c.getSELECTBuilder().append(columnName);

            if (!c.defaultAlias.equals(joinInfo.getFirst())) {
                if (applyAliases) {
                    c.getSELECTBuilder().append(" AS ");
                    c.getSELECTBuilder().append(joinInfo.getFirst());
                    c.getSELECTBuilder().append("_");
                    c.getSELECTBuilder().append(columnName);
                }
                c.createJoinFetch(field);
            }
        }
    }

    private String fetchEffectiveColumnName(Column field, Tuple<String, EntityDescriptor> joinInfo) {
        if (Entity.ID.getName().equals(field.getName()) || Entity.VERSION.getName().equals(field.getName())) {
            return field.getName();
        } else {
            return joinInfo.getSecond().getProperty(field.getName()).getColumnName();
        }
    }

    private Compiler selectCount() {
        Compiler c = new Compiler(descriptor);
        if (!fields.isEmpty()) {
            c.getSELECTBuilder().append("SELECT COUNT(");
            if (distinct) {
                c.getSELECTBuilder().append("DISTINCT");
            }
            appendFieldList(c, false);
            c.getSELECTBuilder().append(")");
        } else {
            c.getSELECTBuilder().append("SELECT COUNT(*)");
        }
        return c;
    }

    private void from(Compiler compiler) {
        compiler.getSELECTBuilder().append(" FROM ").append(descriptor.getTableName()).append(" e");
    }

    private void where(Compiler compiler) {
        boolean hasConstraints = false;
        for (Constraint c : constaints) {
            if (c.addsConstraint()) {
                hasConstraints = true;
                break;
            }
        }
        if (!hasConstraints) {
            return;
        }
        compiler.getWHEREBuilder().append(" WHERE ");
        Monoflop mf = Monoflop.create();
        for (Constraint c : constaints) {
            if (c.addsConstraint()) {
                if (mf.successiveCall()) {
                    compiler.getWHEREBuilder().append(" AND ");
                }
                c.appendSQL(compiler);
            }
        }
    }

    private void orderBy(Compiler compiler) {
        if (!orderBys.isEmpty()) {
            compiler.getWHEREBuilder().append(" ORDER BY ");
            Monoflop mf = Monoflop.create();
            for (Tuple<Column, Boolean> e : orderBys) {
                if (mf.successiveCall()) {
                    compiler.getWHEREBuilder().append(", ");
                }
                compiler.getWHEREBuilder().append(compiler.translateColumnName(e.getFirst()));
                compiler.getWHEREBuilder().append(e.getSecond() ? " ASC" : " DESC");
            }
        }
    }

    private void limit(Compiler compiler) {
        if (limit > 0 && db.hasCapability(Capability.LIMIT)) {
            if (skip > 0) {
                compiler.getWHEREBuilder().append(" LIMIT ").append(skip).append(", ").append(limit);
            } else {
                compiler.getWHEREBuilder().append(" LIMIT ").append(limit);
            }
        }
    }

    @Override
    public String toString() {
        return compileSELECT().toString();
    }
}
