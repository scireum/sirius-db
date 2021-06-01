/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.jdbc.constraints.SQLConstraint;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.properties.SQLEntityRefProperty;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Timeout;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.health.Microtiming;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Provides a query DSL which is used to query {@link SQLEntity} instances from the database.
 *
 * @param <E> the generic type of entities being queried
 */
public class SmartQuery<E extends SQLEntity> extends Query<SmartQuery<E>, E, SQLConstraint> {

    @ConfigValue("jdbc.queryIterateTimeout")
    private static Duration queryIterateTimeout;

    @Part
    private static OMA oma;

    @Part
    private static Databases dbs;

    protected List<Mapping> fields = Collections.emptyList();
    protected boolean distinct;
    protected List<Tuple<Mapping, Boolean>> orderBys = new ArrayList<>();
    protected List<SQLConstraint> constraints = new ArrayList<>();
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

    @Override
    public SmartQuery<E> where(SQLConstraint constraint) {
        if (constraint != null) {
            this.constraints.add(constraint);
        }

        return this;
    }

    @Override
    public FilterFactory<SQLConstraint> filters() {
        return oma.filters();
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
        this.fields = Arrays.asList(fields);
        return this;
    }

    @Override
    public long count() {
        if (forceFail) {
            return 0;
        }
        Watch w = Watch.start();
        Compiler compiler = compileCOUNT();
        try {
            try (Connection c = db.getConnection()) {
                return execCount(compiler, c);
            } finally {
                if (Microtiming.isEnabled()) {
                    w.submitMicroTiming("OMA", "COUNT: " + compiler.getQuery());
                }
            }
        } catch (Exception e) {
            throw queryError(compiler, e);
        }
    }

    protected HandledException queryError(Compiler compiler, Exception e) {
        return Exceptions.handle()
                         .to(OMA.LOG)
                         .error(e)
                         .withSystemErrorMessage("Error executing query '%s' for type '%s': %s (%s)",
                                                 compiler,
                                                 descriptor.getType().getName())
                         .handle();
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
        if (forceFail) {
            return false;
        }
        return copy().fields(SQLEntity.ID).first().isPresent();
    }

    /**
     * Deletes all matches using the {@link OMA#delete(SQLEntity)}.
     * <p>
     * Note that for very large result sets, we perform a blockwise strategy. We therefore iterate over
     * the results until the timeout ({@link #queryIterateTimeout} is reached). In this case, we abort the
     * iteration, execute the query again and continue deleting until all entities are gone.
     *
     * @param entityCallback a callback to be invoked for each entity to be deleted
     */
    @Override
    public void delete(@Nullable Consumer<E> entityCallback) {
        if (forceFail) {
            return;
        }
        AtomicBoolean continueDeleting = new AtomicBoolean(true);
        TaskContext context = TaskContext.get();
        while (continueDeleting.get() && context.isActive()) {
            continueDeleting.set(false);
            Timeout timeout = new Timeout(queryIterateTimeout);
            iterate(entity -> {
                if (entityCallback != null) {
                    entityCallback.accept(entity);
                }
                oma.delete(entity);
                if (timeout.isReached()) {
                    // Timeout has been reached, set the flag so that another delete query is attempted....
                    continueDeleting.set(true);
                    // and abort processing the results of this query...
                    return false;
                } else {
                    // Timeout not yet reached, continue deleting...
                    return true;
                }
            });
        }
    }

    /**
     * Calls the given function on all items in the result, as long as it returns <tt>true</tt>.
     * <p>
     * In contrast to {@link #iterate(Predicate)}, this method is suitable for large result sets or long processing
     * times. As <tt>iterate</tt> keeps the JDBC <tt>ResultSet</tt> open, the underlying server might discard the
     * result at some point in time (e.g. MySQL does this after 30-45min). Therefore, we execute the query and start
     * processing. If a timeout ({@link #queryIterateTimeout} is reached, we stop iterating, discard the result set
     * and emit another query which starts just where the previous query stopped.
     * <p>
     * Note however, as we either do this by ID or by setting an appropriate LIMIT range, there is a possibility,
     * that we either miss an entity or even process an entity twice if a concurrent modification happens, which
     * then changes the result set of this query.
     *
     * @param handler the handler to be invoked for each item in the result. Should return <tt>true</tt>
     *                to continue processing or <tt>false</tt> to abort processing of the result set.
     */
    public void iterateBlockwise(Predicate<E> handler) {
        if (forceFail) {
            return;
        }
        if (orderBys.isEmpty()) {
            iterateBlockwiseById(handler);
        } else {
            iterateBlockwiseByPaging(handler);
        }
    }

    /**
     * Calls the given function on all items in the result.
     *
     * @param handler the handler to be invoked for each item in the result
     * @see #iterateBlockwise(Predicate)
     */
    public void iterateBlockwiseAll(Consumer<E> handler) {
        iterateBlockwise(entity -> {
            handler.accept(entity);
            return true;
        });
    }

    /**
     * Provides a blockwise strategy based on the {@link SQLEntity#ID} of the entities.
     * <p>
     * If there are no ORDER BY clauses present, we can sort by ID and remember the last processed ID before
     * attempting another query.
     *
     * @param handler the handler to be invoked for each item in the result. Should return <tt>true</tt>
     *                to continue processing or <tt>false</tt> to abort processing of the result set.
     */
    private void iterateBlockwiseById(Predicate<E> handler) {
        AtomicLong lastId = new AtomicLong(-1);
        AtomicBoolean keepGoing = new AtomicBoolean(true);
        TaskContext context = TaskContext.get();
        while (keepGoing.get() && context.isActive()) {
            keepGoing.set(false);
            Timeout timeout = new Timeout(queryIterateTimeout);

            // Creates a copy and start processing results just after the results we have processed with the
            // previous query...
            copy().orderAsc(SQLEntity.ID).where(OMA.FILTERS.gt(SQLEntity.ID, lastId.get())).iterate(entity -> {
                if (!handler.test(entity)) {
                    // As soon as the handler returns false, we're done and can abort entirely...
                    return false;
                }

                if (timeout.isReached()) {
                    // If the timeout is reached, we set a flag so that another query will be attempted....
                    keepGoing.set(true);
                    // We remember the ID of the last processed entity, so that the query starts right after
                    // this one...
                    lastId.set(entity.getId());
                    // Abort local processing so that another result set is opened.
                    return false;
                }

                // Timeout not reached yet, continue processing results...
                return true;
            });
        }
    }

    /**
     * Provides a blockwise strategy based on the LIMIT range.
     * <p>
     * If ORDER BY clauses are present, we have to employ a sliding window technique.
     *
     * @param handler the handler to be invoked for each item in the result. Should return <tt>true</tt>
     *                to continue processing or <tt>false</tt> to abort processing of the result set.
     */
    private void iterateBlockwiseByPaging(Predicate<E> handler) {
        // Contains the counter of already processed entities. These have to be skipped when emitting the
        // next query...
        AtomicInteger skipCounter = new AtomicInteger(0);
        AtomicBoolean keepGoing = new AtomicBoolean(true);
        TaskContext context = TaskContext.get();
        while (keepGoing.get() && context.isActive()) {
            keepGoing.set(false);
            Timeout timeout = new Timeout(queryIterateTimeout);
            // Create a copy of the query an install an appropriate skip value...
            copy().skip(skipCounter.get()).iterate(entity -> {
                if (!handler.test(entity)) {
                    // As soon as the handler returns false, we're done and can abort entirely...
                    return false;
                }

                // Remember the processed entity...
                skipCounter.incrementAndGet();

                if (timeout.isReached()) {
                    keepGoing.set(true);
                    return false;
                }

                return true;
            });
        }
    }

    @Override
    public void truncate() {
        throw new UnsupportedOperationException(
                "Truncate is not supported by OMA. Use as OMA.deleteStatement or SmartQuery.delete()");
    }

    /**
     * Converts this query into a plain {@link SQLQuery} which will return rows instead of entities.
     *
     * @return the query converted into a plain SQL query.
     */
    public SQLQuery asSQLQuery() {
        if (forceFail) {
            throw new IllegalStateException("A failed query can not be converted into a SQL query.");
        }
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
        copy.forceFail = forceFail;
        copy.fields = new ArrayList<>(fields);
        copy.orderBys.addAll(orderBys);
        copy.constraints.addAll(constraints);

        return copy;
    }

    @Override
    public void iterate(Predicate<E> handler) {
        if (forceFail) {
            return;
        }
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
                    w.submitMicroTiming("OMA", "ITERATE: " + compiler.getQuery());
                }
            }
        } catch (Exception e) {
            throw queryError(compiler, e);
        }
    }

    @SuppressWarnings("unchecked")
    protected void execIterate(Predicate<E> handler, Compiler compiler, Limit limit, boolean nativeLimit, ResultSet rs)
            throws Exception {
        TaskContext tc = TaskContext.get();
        Set<String> columns = dbs.readColumns(rs);
        while (rs.next() && tc.isActive()) {
            if (nativeLimit || limit.nextRow()) {
                SQLEntity e = makeEntity(descriptor, null, columns, rs);
                compiler.executeJoinFetches(e, columns, rs);
                if (!handler.test((E) e)) {
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
        SQLEntity result = (SQLEntity) descriptor.make(OMA.class, alias, key -> {
            try {
                return columns.contains(key.toUpperCase()) ? Value.of(rs.getObject(key)) : null;
            } catch (SQLException e) {
                throw Exceptions.handle(OMA.LOG, e);
            }
        });

        if (descriptor.isVersioned() && columns.contains(BaseMapper.VERSION.toUpperCase())) {
            result.setVersion(rs.getInt(BaseMapper.VERSION.toUpperCase()));
        }

        return result;
    }

    protected void tuneStatement(PreparedStatement stmt, Limit limit, boolean nativeLimit) throws SQLException {
        if (!nativeLimit && limit.getTotalItems() > 0) {
            stmt.setMaxRows(limit.getTotalItems());
        }
        if (limit.getTotalItems() > SQLQuery.DEFAULT_FETCH_SIZE || limit.getTotalItems() <= 0) {
            if (db.hasCapability(Capability.STREAMING)) {
                stmt.setFetchSize(Integer.MIN_VALUE);
            } else {
                stmt.setFetchSize(SQLQuery.DEFAULT_FETCH_SIZE);
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
            Map<String, JoinFetch> subFetches = new TreeMap<>();
        }

        protected EntityDescriptor ed;
        protected StringBuilder preJoinQuery = new StringBuilder();
        protected StringBuilder joins = new StringBuilder();
        protected StringBuilder postJoinQuery = new StringBuilder();
        protected List<Object> parameters = new ArrayList<>();
        protected Map<String, Tuple<String, EntityDescriptor>> joinTable = new TreeMap<>();
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
                 .append(parentAlias.getSecond().rewritePropertyName(parent.getName()));
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
            List<Mapping> fetchPath = new ArrayList<>();
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
        if (constraints.isEmpty()) {
            return;
        }
        compiler.getWHEREBuilder().append(" WHERE ");
        Monoflop mf = Monoflop.create();
        for (SQLConstraint c : constraints) {
            if (mf.successiveCall()) {
                compiler.getWHEREBuilder().append(" AND ");
            }
            c.appendSQL(compiler);
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
                compiler.getWHEREBuilder().append(Boolean.TRUE.equals(e.getSecond()) ? " ASC" : " DESC");
            }
        }
    }

    private void limit(Compiler compiler) {
        //if a skip value is set but no limit, we have to set a limit > 0 to make limit working.
        int effectiveLimit = limit == 0 && skip > 0 ? Integer.MAX_VALUE : limit;
        if (effectiveLimit > 0 && db.hasCapability(Capability.LIMIT)) {
            if (skip > 0) {
                compiler.getWHEREBuilder().append(" LIMIT ").append(skip).append(", ").append(effectiveLimit);
            } else {
                compiler.getWHEREBuilder().append(" LIMIT ").append(effectiveLimit);
            }
        }
    }

    @Override
    public String toString() {
        return compileSELECT().toString();
    }
}
