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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by aha on 29.11.14.
 */
public class SmartQuery<E extends Entity> extends BaseQuery<E> {
    protected final EntityDescriptor descriptor;
    protected List<Column> fields = Collections.emptyList();
    protected boolean distinct;
    protected List<Tuple<Column, Boolean>> orderBys = Lists.newArrayList();
    protected List<Constraint> containts = Lists.newArrayList();
    protected Database db;

    protected SmartQuery(Class<E> type, Database db) {
        super(type);
        this.db = db;
        this.descriptor = getDescriptor();
    }

    @Override
    public SmartQuery<E> skip(int skip) {
        return (SmartQuery<E>) super.skip(skip);
    }

    @Override
    public SmartQuery<E> limit(int limit) {
        return (SmartQuery<E>) super.limit(limit);
    }

    public SmartQuery<E> where(Constraint... constraints) {
        Collections.addAll(this.containts, constraints);

        return this;
    }

    public SmartQuery<E> eq(Column field, Object value) {
        this.containts.add(FieldOperator.on(field).eq(value));
        return this;
    }

    public SmartQuery<E> eqIgnoreNull(Column field, Object value) {
        this.containts.add(FieldOperator.on(field).eq(value).ignoreNull());
        return this;
    }

    public SmartQuery<E> orderAsc(Column field) {
        orderBys.add(Tuple.create(field, true));
        return this;
    }

    public SmartQuery<E> orderDesc(Column field) {
        orderBys.add(Tuple.create(field, false));
        return this;
    }

    public SmartQuery<E> distinctFields(Column... fields) {
        this.fields = Arrays.asList(fields);
        this.distinct = true;
        return this;
    }

    public SmartQuery<E> fields(Column... fields) {
        this.fields = Arrays.asList(fields);
        return this;
    }

    public long count() {
        Watch w = Watch.start();
        Compiler compiler = compileCOUNT();
        try {
            try (Connection c = db.getConnection()) {
                try (PreparedStatement stmt = compiler.prepareStatement(c)) {
                    if (stmt == null) {
                        return 0;
                    }
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            return rs.getLong(1);
                        } else {
                            return 0;
                        }
                    }
                }
            } finally {
                if (Microtiming.isEnabled()) {
                    w.submitMicroTiming("OMA", compiler.toString());
                }
            }
        } catch (Throwable e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Error executing query '%s' for type '%s': %s (%s)",
                                                    compiler,
                                                    type.getName())
                            .handle();
        }
    }

    public boolean exists() {
        return count() > 0;
    }

    @Part
    private static OMA oma;

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

    public SQLQuery asSQLQuery() {
        Compiler compiler = compileSELECT();
        return new SQLQuery(db, compiler.getQuery()) {
            @Override
            protected PreparedStatement createPreparedStatement(Connection c) throws SQLException {
                return compiler.prepareStatement(c);
            }
        };
    }

    public SmartQuery<E> copy() {
        SmartQuery<E> copy = new SmartQuery<>(type, db);
        copy.distinct = distinct;
        copy.fields.addAll(fields);
        copy.orderBys.addAll(orderBys);
        copy.containts.addAll(containts);

        return copy;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(Function<E, Boolean> handler) {
        Compiler compiler = compileSELECT();
        try {
            Watch w = Watch.start();
            try (Connection c = db.getConnection()) {
                try (PreparedStatement stmt = compiler.prepareStatement(c)) {
                    if (stmt == null) {
                        return;
                    }
                    Limit limit = getLimit();
                    boolean nativeLimit = db.hasCapability(Capability.LIMIT);
                    tuneStatement(stmt, limit, nativeLimit);
                    try (ResultSet rs = stmt.executeQuery()) {
                        TaskContext tc = TaskContext.get();
                        Set<String> columns = readColumns(rs);
                        while (rs.next() && tc.isActive()) {
                            if (nativeLimit || limit.nextRow()) {
                                Entity e = descriptor.readFrom(null, columns, rs);
                                compiler.executeJoinFetches(e, columns, rs);
                                if (!handler.apply((E) e)) {
                                    break;
                                }
                            }
                            if (!nativeLimit && !limit.shouldContinue()) {
                                break;
                            }
                        }
                    }
                }
            } finally {
                if (Microtiming.isEnabled()) {
                    w.submitMicroTiming("OMA", compiler.toString());
                }
            }
        } catch (Throwable e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Error executing query '%s' for type '%s': %s (%s)",
                                                    compiler.toString(),
                                                    type.getName())
                            .handle();
        }
    }

    protected void tuneStatement(PreparedStatement stmt, Limit limit, boolean nativeLimit) throws SQLException {
        if (!nativeLimit && limit.getTotalItems() > 0) {
            stmt.setMaxRows(limit.getTotalItems());
        }
        if (db.hasCapability(Capability.STREAMING) && (limit.getTotalItems() > 1000 || limit.getTotalItems() <= 0)) {
            stmt.setFetchSize(Integer.MIN_VALUE);
        } else {
            stmt.setFetchSize(1000);
        }
    }

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
        private JoinFetch rootFetch = new JoinFetch();

        public Compiler(@Nullable EntityDescriptor ed) {
            this.ed = ed;
        }

        public StringBuilder getSELECTBuilder() {
            return preJoinQuery;
        }

        public StringBuilder getWHEREBuilder() {
            return postJoinQuery;
        }

        private Tuple<String, EntityDescriptor> determineAlias(Column parent) {
            if (parent == null || ed == null) {
                return Tuple.create("e", ed);
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
            String tableAlias = "t" + joinTable.size();
            joins.append(" LEFT JOIN ")
                 .append(other.getTableName())
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

        public String translateColumnName(Column column) {
            String alias = determineAlias(column.getParent()).getFirst();
            return alias + "." + ed.rewriteColumnName(column.getName());
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
                currentDescriptor = subFetch.property.getDescriptor();
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
            } catch (Throwable e) {
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
            c.getSELECTBuilder().append(" e.*");
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
            String alias = c.determineAlias(field.getParent()).getFirst();
            if ("e".equals(alias)) {
                c.getSELECTBuilder().append(field);
            } else {
                c.getSELECTBuilder().append(alias);
                c.getSELECTBuilder().append(".");
                c.getSELECTBuilder().append(field.getName());
                if (applyAliases) {
                    c.getSELECTBuilder().append(" AS ");
                    c.getSELECTBuilder().append(alias);
                    c.getSELECTBuilder().append("_");
                    c.getSELECTBuilder().append(field.getName());
                }
                c.createJoinFetch(field);
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
        compiler.getSELECTBuilder().append(" FROM ").append(descriptor.getTableName()).append(" e");
    }

    private void where(Compiler compiler) {
        boolean hasConstraints = false;
        for (Constraint c : containts) {
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
        for (Constraint c : containts) {
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
