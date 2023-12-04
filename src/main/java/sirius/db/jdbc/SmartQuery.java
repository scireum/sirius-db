/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.jdbc.constraints.SQLConstraint;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.properties.BaseEntityRefProperty;
import sirius.db.mixing.properties.SQLEntityRefProperty;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.PullBasedSpliterator;
import sirius.kernel.commons.Strings;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    protected List<String> aggregationFields = Collections.emptyList();
    protected boolean distinct;
    protected List<Tuple<Mapping, Boolean>> orderBys = new ArrayList<>();
    protected List<String> groupBys = Collections.emptyList();
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

    /**
     * Adds a complex expression like an aggregation function to the <tt>SELECT</tt> clause of the generated SQL.
     * <p>
     * In contrast to {@link #fields(Mapping...)}, this adds an expression but does not replace the previous ones
     * (neither those added via {@link #fields(Mapping...)} nor other {@link #aggregationField(String)} calls).
     * Therefore, this can be used to add fields or aggregations conditionally.
     * <p>
     * <b>NOTE:</b> This cannot be used in "normal" entity queries, as the O/R mapper cannot handle aggregations.
     * Rather, the query has to be converted using {@link #asSQLQuery()} which then permits direct access to rows.
     * <p>
     * <b>ALSO NOTE:</b> The given expressions will directly end up on the SQL query and must therefore be constant
     * and safe string which aren't subject to SQL injection attacks!
     *
     * @param expression the expression to group by
     * @return the query itself for fluent method calls
     * @see #groupBy(String)
     * @see #asSQLQuery()
     */
    public SmartQuery<E> aggregationField(String expression) {
        if (this.aggregationFields.isEmpty()) {
            this.aggregationFields = new ArrayList<>();
        }

        this.aggregationFields.add(expression);
        return this;
    }

    /**
     * Adds an expression to the <tt>GROUP BY</tt> clause of the generated SQL.
     * <p>
     * <b>NOTE:</b> This cannot be used in "normal" entity queries, as the O/R mapper cannot handle aggregations.
     * Rather, the query has to be converted using {@link #asSQLQuery()} which then permits direct access to rows.
     * <p>
     * <b>ALSO NOTE:</b> The given expressions will directly end up on the SQL query and must therefore be constant
     * and safe string which aren't subject to SQL injection attacks!
     *
     * @param expression the expression to group by
     * @return the query itself for fluent method calls
     * @see #aggregationField(String)
     * @see #asSQLQuery()
     */
    public SmartQuery<E> groupBy(String expression) {
        if (this.groupBys.isEmpty()) {
            this.groupBys = new ArrayList<>();
        }

        this.groupBys.add(expression);
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
     * Deletes all matches using the {@link OMA#delete(BaseEntity)}.
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
        TaskContext taskContext = TaskContext.get();
        while (continueDeleting.get() && taskContext.isActive()) {
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

    @Override
    public Stream<E> streamBlockwise() {
        if (forceFail) {
            return Stream.empty();
        }
        return StreamSupport.stream(new SmartQuerySpliterator(), false);
    }

    private class SmartQuerySpliterator extends PullBasedSpliterator<E> {
        private E lastValue = null;
        private List<Object> orderByValuesOfLastEntityDuringFetch = null;
        private final TaskContext taskContext = TaskContext.get();
        private final SmartQuery<E> adjustedQuery;

        private SmartQuerySpliterator() {
            adjustedQuery = adjustQuery(SmartQuery.this);
        }

        @Override
        public int characteristics() {
            return NONNULL | IMMUTABLE | ORDERED;
        }

        @Override
        protected Iterator<E> pullNextBlock() {
            if (!taskContext.isActive()) {
                return null;
            }

            List<E> block = queryNextBlock();
            if (!block.isEmpty()) {
                lastValue = block.get(block.size() - 1);
                orderByValuesOfLastEntityDuringFetch = extractOrderByValues(lastValue);
            }
            return block.iterator();
        }

        private SmartQuery<E> adjustQuery(SmartQuery<E> query) {
            SmartQuery<E> adjusted = query.copy();

            if (adjusted.limit > 0) {
                throw new UnsupportedOperationException("SmartQuery doesn't allow 'limit' in streamBlockwise");
            }
            if (adjusted.skip > 0) {
                throw new UnsupportedOperationException("SmartQuery doesn't allow 'skip' in streamBlockwise");
            }

            // we need to guarantee an absolute ordering
            if (adjusted.distinct) {
                // we have distinct fields, so we can easily create an absolute ordering, if it's not already there
                adjusted.fields.stream()
                               .filter(Predicate.not(new HashSet<>(Tuple.firsts(orderBys))::contains))
                               .forEach(adjusted::orderAsc);
            } else {
                // we are not DISTINCT, so we can easily guarantee an absolute ordering using the ID
                adjusted.orderAsc(BaseEntity.ID);
            }

            if (!adjusted.distinct && !fields.isEmpty()) {
                // We SELECT a subset of the columns to optimize the network bandwidth.
                // When pulling the next block, we need to continue exactly where we left of, so we need to SELECT
                // at least all the fields from the ORDER BY clause.
                Set<Mapping> allFields = new HashSet<>(adjusted.fields);
                allFields.addAll(Tuple.firsts(adjusted.orderBys));
                adjusted.fields(allFields.toArray(Mapping[]::new));
            }

            return adjusted;
        }

        private List<Object> extractOrderByValues(E entity) {
            return Tuple.firsts(adjustedQuery.orderBys).stream().map(field -> getPropertyValue(field, entity)).toList();
        }

        private List<E> queryNextBlock() {
            SmartQuery<E> effectiveQuery = adjustedQuery.copy().limit(MAX_LIST_SIZE);

            if (lastValue == null) {
                return effectiveQuery.queryList();
            }

            List<Object> orderByValuesOfLastEntity = extractOrderByValues(lastValue);
            if (!orderByValuesOfLastEntityDuringFetch.equals(orderByValuesOfLastEntity)) {
                throw new IllegalStateException(Strings.apply(
                        "Entity '%s' was changed while streaming over it. This is very likely to cause bad result sets, including infinity loops, and is not supported.\nPrevious values: %s\nCurrent values: %s",
                        lastValue,
                        orderByValuesOfLastEntityDuringFetch,
                        orderByValuesOfLastEntity));
            }

            SQLConstraint sortingFilterConstraint = null;
            Map<Mapping, Object> previousSortingColumns = new HashMap<>();
            for (Tuple<Mapping, Boolean> sorting : effectiveQuery.orderBys) {
                Mapping sortColumn = sorting.getFirst();
                boolean sortAscending = sorting.getSecond().booleanValue();
                Object value = getPropertyValue(sortColumn, lastValue);

                SQLConstraint currentColumConstraint =
                        createSqlConstraintForSortingColumn(sortAscending, sortColumn, value, previousSortingColumns);
                sortingFilterConstraint = OMA.FILTERS.or(sortingFilterConstraint, currentColumConstraint);

                previousSortingColumns.put(sortColumn, value);
            }
            return effectiveQuery.where(sortingFilterConstraint).queryList();
        }

        private Object getPropertyValue(Mapping mapping, BaseEntity<?> entity) {
            BaseEntity<?> parent = findParent(mapping, entity);
            return parent.getDescriptor().getProperty(mapping.getName()).getValue(parent);
        }

        private BaseEntity<?> findParent(Mapping mapping, BaseEntity<?> entity) {
            if (mapping.getParent() != null) {
                BaseEntity<?> parentEntity = findParent(mapping.getParent(), entity);
                if (parentEntity.getDescriptor()
                                .getProperty(mapping.getParent()
                                                    .getName()) instanceof BaseEntityRefProperty<?, ?, ?> ref) {
                    BaseEntityRef<?, ?> entityRef = ref.getEntityRef(parentEntity);
                    return entityRef.getValueIfPresent().orElseThrow(() -> {
                        return new IllegalArgumentException(Strings.apply(
                                "The BaseEntityRef `%s` is not loaded, but is requested by the mapping `%s`.",
                                entityRef.getUniqueObjectName(),
                                mapping.getParent()));
                    });
                } else {
                    throw new IllegalArgumentException(Strings.apply("You cannot join on the non-ref property `%s`",
                                                                     mapping.getParent()));
                }
            }
            return entity;
        }
    }

    /**
     * Creates a sql constraint for sorting purposes.
     * </p>
     * In MySQL/MariaDB, NULL is considered as a 'missing, unknown value'. Any arithmetic comparison with NULL
     * returns false e.g. NULL != 'any' returns false.
     * Therefore, comparisons with NULL values must be treated specially.
     *
     * @param sortAscending          decides whether the sorting direction is descending or ascending
     * @param column                 the column to be used for sorting
     * @param value                  the value of the column
     * @param previousSortingColumns all columns that should be sorted before the current one
     * @return {@link SQLConstraint} which can be used to map a level of sorting.
     */
    SQLConstraint createSqlConstraintForSortingColumn(boolean sortAscending,
                                                      Mapping column,
                                                      Object value,
                                                      Map<Mapping, Object> previousSortingColumns) {
        SQLConstraint sortingStep = null;
        for (Map.Entry<Mapping, Object> previousColumn : previousSortingColumns.entrySet()) {
            sortingStep =
                    OMA.FILTERS.and(sortingStep, OMA.FILTERS.eq(previousColumn.getKey(), previousColumn.getValue()));
        }

        if (value == null) {
            return createConstraintForSortingWithNull(sortAscending, sortingStep, column);
        }
        if (nullValuesFirst(sortAscending)) {
            return OMA.FILTERS.and(sortingStep, OMA.FILTERS.gt(column, value));
        } else {
            return OMA.FILTERS.and(sortingStep,
                                   OMA.FILTERS.or(OMA.FILTERS.lt(column, value), OMA.FILTERS.eq(column, null)));
        }
    }

    private SQLConstraint createConstraintForSortingWithNull(boolean sortAscending,
                                                             SQLConstraint sortingStep,
                                                             Mapping column) {
        if (nullValuesFirst(sortAscending)) {
            return OMA.FILTERS.and(sortingStep, OMA.FILTERS.ne(column, null));
        }
        return null;
    }

    /**
     * Indicates whether null values are listed before or after non-null values when executing this query.
     * <p>
     * Both the sort order and the implementation in the database tell us whether we will get a
     * result where the null values are at the beginning or at the end.
     *
     * @param sortAscending decides whether the sorting direction is descending or ascending
     * @return {@code true} if the sorted list starts with null values
     */
    private boolean nullValuesFirst(boolean sortAscending) {
        if (db == null) {
            // should be only true in tests
            return sortAscending;
        }
        if (sortAscending) {
            return db.hasCapability(Capability.NULLS_FIRST);
        } else {
            return !db.hasCapability(Capability.NULLS_FIRST);
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
        copy.limit = limit;
        copy.skip = skip;

        return copy;
    }

    @Override
    protected void doIterate(Predicate<E> handler) {
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
    protected void execIterate(Predicate<E> handler,
                               Compiler compiler,
                               Limit limit,
                               boolean nativeLimit,
                               ResultSet resultSet) throws Exception {
        Set<String> columns = dbs.readColumns(resultSet);
        while (resultSet.next()) {
            if (nativeLimit || limit.nextRow()) {
                SQLEntity entity = makeEntity(descriptor, null, columns, resultSet);
                compiler.executeJoinFetches(entity, columns, resultSet);
                if (!handler.test((E) entity)) {
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
                stmt.setFetchSize(1);
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
                Databases.convertAndSetParameter(stmt, i + 1, parameters.get(i));
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
        groupBy(compiler);
        orderBy(compiler);
        limit(compiler);
        return compiler;
    }

    private Compiler compileCOUNT() {
        Compiler compiler = selectCount();
        from(compiler);
        where(compiler);
        groupBy(compiler);
        return compiler;
    }

    private Compiler select() {
        Compiler c = new Compiler(descriptor);
        c.getSELECTBuilder().append("SELECT ");
        if (fields.isEmpty() && aggregationFields.isEmpty()) {
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
        requiredFields.forEach(requiredField -> appendToSELECT(c, applyAliases, mf, requiredField, false, null));

        for (String aggregationField : aggregationFields) {
            if (mf.successiveCall()) {
                c.getSELECTBuilder().append(", ");
            }
            c.getSELECTBuilder().append(aggregationField);
        }
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
        c.getSELECTBuilder().append("SELECT COUNT(");

        if (fields.isEmpty() || (fields.size() == 1 && !distinct)) {
            c.getSELECTBuilder().append("*");
        } else if (distinct) {
            c.getSELECTBuilder().append("DISTINCT ");
            appendFieldList(c, false);
        } else {
            throw Exceptions.createHandled()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("Only use multiple arguments in 'fields' and 'count' in "
                                                    + "combination with the 'distinct' statement")
                            .handle();
        }

        c.getSELECTBuilder().append(")");
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

    private void groupBy(Compiler compiler) {
        if (!groupBys.isEmpty()) {
            compiler.getWHEREBuilder().append(" GROUP BY ");
            compiler.getWHEREBuilder().append(Strings.join(groupBys, ", "));
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
