/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import sirius.db.jdbc.constraints.FieldOperator;
import sirius.db.jdbc.properties.SQLEntityRefProperty;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Query;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Provides a query DSL which is used to query {@link SQLEntity} instances from the database.
 *
 * @param <E> the generic type of entities being queried
 */
public class SmartQuery<E extends SQLEntity> extends Query<SmartQuery<E>, E> {

    @Part
    private static OMA oma;

    @Part
    private static Databases dbs;

    protected List<Mapping> fields = Collections.emptyList();
    protected boolean distinct;
    protected List<Tuple<Mapping, Boolean>> orderBys = Lists.newArrayList();
    protected List<Constraint> constaints = Lists.newArrayList();
    protected Database db;

    /**
     * Creates a new query instance.
     * <p>
     * Use {@link OMA#select(Class)} to create a new query.
     *
     * @param descriptor the descriptor of the type to query
     * @param db         the database to operate on
     */
    protected SmartQuery(EntityDescriptor descriptor, Database db) {
        super(descriptor);
        this.db = db;
    }

    /**
     * Returns the <tt>EntityDescriptor</tt> for the type of entities addressed by this query.
     *
     * @return the underlying entity descriptor of this query.
     */
    public EntityDescriptor getEntityDescriptor() {
        return descriptor;
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

    @Override
    public SmartQuery<E> eq(Mapping field, Object value) {
        this.constaints.add(FieldOperator.on(field).eq(value));
        return this;
    }

    @Override
    public SmartQuery<E> eqIgnoreNull(Mapping field, Object value) {
        this.constaints.add(FieldOperator.on(field).eq(value).ignoreNull());
        return this;
    }

    @Override
    public SmartQuery<E> orderAsc(Mapping field) {
        orderBys.add(Tuple.create(field, true));
        return this;
    }

    @Override
    public SmartQuery<E> orderDesc(Mapping field) {
        orderBys.add(Tuple.create(field, false));
        return this;
    }

    /**
     * Specifies the fields to select, which also have to be <tt>DISTINCT</tt>.
     *
     * @param fields the fields to select and to apply a <tt>DISTINCT</tt> filter on.
     * @return the query itself for fluent method calls
     */
    public SmartQuery<E> distinctFields(Mapping... fields) {
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
    public SmartQuery<E> fields(Mapping... fields) {
        this.fields = Lists.newArrayList(fields);
        return this;
    }

    @Override
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
                                                    descriptor.getType().getName())
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

    @Override
    public boolean exists() {
        return count() > 0;
    }

    /**
     * Deletes all entities matching this query.
     * <p>
     * Note that this will not generate a <tt>DELETE</tt> statement but rather select the results and invoke
     * {@link OMA#delete(sirius.db.mixing.BaseEntity)} on each entity to ensure that framework checks are triggered.
     */
    public void delete() {
        iterateAll(oma::delete);
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
        SmartQuery<E> copy = new SmartQuery<>(descriptor, db);
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
                                                    descriptor.getType().getName())
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
        Set<String> columns = dbs.readColumns(rs);
        while (rs.next() && tc.isActive()) {
            if (nativeLimit || limit.nextRow()) {
                SQLEntity e = makeEntity(descriptor, null, columns, rs);
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

    private static SQLEntity makeEntity(EntityDescriptor descriptor, String alias, Set<String> columns, ResultSet rs)
            throws Exception {
        SQLEntity result = (SQLEntity) descriptor.make(alias, key -> {
            try {
                return columns.contains(key.toUpperCase()) ? Value.of(rs.getObject(key)) : null;
            } catch (SQLException e) {
                throw Exceptions.handle(OMA.LOG, e);
            }
        });

        if (descriptor.isVersioned()) {
            result.setVersion(rs.getInt(BaseMapper.VERSION));
        }

        return result;
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
            SQLEntityRefProperty property;
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
        protected JoinFetch rootFetch = new JoinFetch();

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

        public void setWHEREBuilder(StringBuilder newWHEREBuilder) {
            this.postJoinQuery = newWHEREBuilder;
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
         * Returns the currently active translation state and replaces all settings for the new alias and base descriptor.
         * <p>
         * To restore the state, {@link #restoreTranslationState(TranslationState)} can be used.
         *
         * @param newDefaultAlias      specifies the new default alias to use
         * @param newDefaultDescriptor specifies the new main / default descriptor to use
         * @return the currently active translation state
         */
        public TranslationState captureAndReplaceTranslationState(String newDefaultAlias,
                                                                  EntityDescriptor newDefaultDescriptor) {
            TranslationState result = new TranslationState(ed, defaultAlias, joins, joinTable);

            this.defaultAlias = newDefaultAlias;
            this.ed = newDefaultDescriptor;
            this.joins = new StringBuilder();
            this.joinTable = new TreeMap<>();

            return result;
        }

        /**
         * Provides access to the currently generated JOINs.
         *
         * @return the currently generated JOINs
         */
        public StringBuilder getJoins() {
            return joins;
        }

        /**
         * Restores a previously captured translation state.
         *
         * @param state the original state to restore
         */
        public void restoreTranslationState(TranslationState state) {
            this.defaultAlias = state.getDefaultAlias();
            this.ed = state.getEd();
            this.joins = state.getJoins();
            this.joinTable = state.getJoinTable();
        }

        private Tuple<String, EntityDescriptor> determineAlias(Mapping parent) {
            if (parent == null || ed == null) {
                return Tuple.create(defaultAlias, ed);
            }
            String path = parent.toString();
            Tuple<String, EntityDescriptor> result = joinTable.get(path);
            if (result != null) {
                return result;
            }
            Tuple<String, EntityDescriptor> parentAlias = determineAlias(parent.getParent());
            SQLEntityRefProperty refProperty =
                    (SQLEntityRefProperty) parentAlias.getSecond().getProperty(parent.getName());
            EntityDescriptor other = refProperty.getReferencedDescriptor();

            String tableAlias = generateTableAlias();
            joins.append(" LEFT JOIN ")
                 .append(other.getRelationName())
                 .append(" ")
                 .append(tableAlias)
                 .append(" ON ")
                 .append(tableAlias)
                 .append(".id = ")
                 .append(parentAlias.getFirst())
                 .append(".")
                 .append(parent.getName());
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
        public String translateColumnName(Mapping column) {
            Tuple<String, EntityDescriptor> aliasAndDescriptor = determineAlias(column.getParent());
            EntityDescriptor effectiveDescriptor = aliasAndDescriptor.getSecond();
            if (effectiveDescriptor != null) {
                return aliasAndDescriptor.getFirst() + "." + effectiveDescriptor.rewritePropertyName(column.getName());
            } else {
                return aliasAndDescriptor.getFirst() + "." + column.getName();
            }
        }

        private void createJoinFetch(Mapping field, List<Mapping> fields, List<Mapping> requiredColumns) {
            List<Mapping> fetchPath = Lists.newArrayList();
            Mapping parent = field.getParent();
            while (parent != null) {
                fetchPath.add(0, parent);
                parent = parent.getParent();
            }
            JoinFetch jf = rootFetch;
            EntityDescriptor currentDescriptor = ed;
            for (Mapping col : fetchPath) {
                if (!isContainedInFields(col, fields)) {
                    requiredColumns.add(col);
                }
                JoinFetch subFetch = jf.subFetches.get(col.getName());
                if (subFetch == null) {
                    subFetch = new JoinFetch();
                    Tuple<String, EntityDescriptor> parentInfo = determineAlias(col);
                    subFetch.tableAlias = parentInfo.getFirst();
                    subFetch.property = (SQLEntityRefProperty) currentDescriptor.getProperty(col.getName());
                    jf.subFetches.put(col.getName(), subFetch);
                }
                jf = subFetch;
                currentDescriptor = subFetch.property.getReferencedDescriptor();
            }
        }

        private boolean isContainedInFields(Mapping col, List<Mapping> fields) {
            return fields.stream().anyMatch(field -> field.toString().equals(col.toString()));
        }

        protected void executeJoinFetches(SQLEntity entity, Set<String> columns, ResultSet rs) {
            executeJoinFetch(rootFetch, entity, columns, rs);
        }

        private void executeJoinFetch(JoinFetch jf, SQLEntity parent, Set<String> columns, ResultSet rs) {
            try {
                SQLEntity child = parent;
                if (jf.property != null) {
                    child = makeEntity(jf.property.getReferencedDescriptor(), jf.tableAlias, columns, rs);
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

        @SuppressWarnings("squid:S2095")
        @Explain("The statement will be closed by the caller.")
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
        List<Mapping> requiredFields = new ArrayList<>();

        fields.forEach(field -> {
            appendToSELECT(c, applyAliases, mf, field, true, requiredFields);
        });

        // make sure that the join fields are always fetched
        requiredFields.forEach(requiredField -> {
            appendToSELECT(c, applyAliases, mf, requiredField, false, null);
        });
    }

    private void appendToSELECT(Compiler c,
                                boolean applyAliases,
                                Monoflop mf,
                                Mapping field,
                                boolean createJoinFetch,
                                List<Mapping> requiredFieldsCollector) {
        if (mf.successiveCall()) {
            c.getSELECTBuilder().append(", ");
        }
        Tuple<String, EntityDescriptor> joinInfo = c.determineAlias(field.getParent());
        c.getSELECTBuilder().append(joinInfo.getFirst());
        c.getSELECTBuilder().append(".");
        String columnName = joinInfo.getSecond().getProperty(field.getName()).getPropertyName();
        c.getSELECTBuilder().append(columnName);

        if (!c.defaultAlias.equals(joinInfo.getFirst())) {
            if (applyAliases) {
                c.getSELECTBuilder().append(" AS ");
                c.getSELECTBuilder().append(joinInfo.getFirst());
                c.getSELECTBuilder().append("_");
                c.getSELECTBuilder().append(columnName);
            }
            if (createJoinFetch) {
                c.createJoinFetch(field, fields, requiredFieldsCollector);
            }
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
        compiler.getSELECTBuilder().append(" FROM ").append(descriptor.getRelationName()).append(" e");
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
            for (Tuple<Mapping, Boolean> e : orderBys) {
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
