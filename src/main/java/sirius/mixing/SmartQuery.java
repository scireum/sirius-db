/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import sirius.db.jdbc.Capability;
import sirius.db.jdbc.Database;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Microtiming;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by aha on 29.11.14.
 */
public class SmartQuery<E extends Entity> extends BaseQuery<E> {
    protected final EntityDescriptor descriptor;
    protected String[] fields;
    protected List<Tuple<String, Boolean>> orderBys = Lists.newArrayList();
    protected List<Constraint> containts = Lists.newArrayList();
    protected List<Tuple<String, String[]>> joinFetches = Lists.newArrayList();
    protected Database db;

    protected SmartQuery(Class<E> type, Database db) {
        super(type);
        this.db = db;
        this.descriptor = getDescriptor();
    }

    @Override
    public SmartQuery<E> start(int start) {
        return (SmartQuery<E>) super.start(start);
    }

    @Override
    public SmartQuery<E> limit(int limit) {
        return (SmartQuery<E>) super.limit(limit);
    }

    public SmartQuery<E> where(Constraint... constraints) {
        for (Constraint c : constraints) {
            this.containts.add(c);
        }

        return this;
    }

    public SmartQuery<E> orderAsc(String field) {
        orderBys.add(Tuple.create(field, true));
        return this;
    }

    public SmartQuery<E> orderDesc(String field) {
        orderBys.add(Tuple.create(field, false));
        return this;
    }

    public SmartQuery<E> fields(String... fields) {
        this.fields = fields;
        return this;
    }

    public SmartQuery<E> joinFetch(String relation, String... fields) {
        joinFetches.add(Tuple.create(relation, fields));
        return this;
    }

    public long count() {
        return 0;
    }

    protected static Set<String> readColumns(ResultSet rs) throws SQLException {
        Set<String> result = Sets.newHashSet();
        for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
            result.add(rs.getMetaData().getColumnLabel(col).toUpperCase());
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(Function<E, Boolean> handler) {
        try {
            Watch w = Watch.start();
            try (Connection c = db.getConnection()) {
                PreparedStatement stmt = compileStatement(c);
                if (stmt == null) {
                    return;
                }
                Limit limit = getLimit();
                if (limit.getTotalItems() > 0) {
                    stmt.setMaxRows(limit.getTotalItems());
                }
                if (db.hasCapability(Capability.STREAMING) && (limit.getTotalItems() > 1000 || limit.getTotalItems() <= 0)) {
                    stmt.setFetchSize(Integer.MIN_VALUE);
                } else {
                    stmt.setFetchSize(1000);
                }
                try (ResultSet rs = stmt.executeQuery()) {
                    TaskContext tc = TaskContext.get();
                    Set<String> columns = readColumns(rs);
                    while (rs.next() && tc.isActive()) {
                        limit.nextRow();
                        if (limit.shouldOutput()) {
                            Entity e = descriptor.readFrom(null, columns, rs);
                            if (!handler.apply((E) e)) {
                                break;
                            }
                        }
                        if (!limit.shouldContinue()) {
                            break;
                        }
                    }
                } finally {
                    stmt.close();
                }
            } finally {
                if (Microtiming.isEnabled()) {
                    w.submitMicroTiming("OMA", toString());
                }
            }
        } catch (Throwable e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Error executing query '%s' for type '%s': %s (%s)",
                                                    this,
                                                    type.getName())
                            .handle();
        }
    }

    public static class Compiler {

        protected StringBuilder sb = new StringBuilder();
        protected List<Object> parameters = Lists.newArrayList();

        public StringBuilder getSQLBuilder() {
            return sb;
        }

        public void addParameter(Object parameter) {
            parameters.add(parameter);
        }

        private PreparedStatement prepareStatement(Connection c) throws SQLException {
            PreparedStatement stmt = c.prepareStatement(sb.toString(),
                                                        ResultSet.TYPE_FORWARD_ONLY,
                                                        ResultSet.CONCUR_READ_ONLY);
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i + 1, parameters.get(i));
            }
            return stmt;
        }

        @Override
        public String toString() {
            if (parameters.isEmpty()) {
                return sb.toString();
            } else {
                return sb.toString() + " " + parameters;
            }
        }
    }

    private PreparedStatement compileStatement(Connection c) throws SQLException {
        return compile().prepareStatement(c);
    }

    private Compiler compile() {
        Compiler compiler = select();
        from(compiler);
        join(compiler);
        where(compiler);
        orderBy(compiler);
        limit(compiler);
        return compiler;
    }

    private Compiler select() {
        Compiler c = new Compiler();
        c.getSQLBuilder().append("SELECT ").append(" *\n");
        return c;
    }

    private void from(Compiler compiler) {
        compiler.getSQLBuilder().append("   FROM ").append(descriptor.getTableName()).append("\n");
    }

    private void join(Compiler compiler) {

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
        compiler.getSQLBuilder().append("   WHERE ");
        Monoflop mf = Monoflop.create();
        for (Constraint c : containts) {
            if (c.addsConstraint()) {
                if (mf.successiveCall()) {
                    compiler.getSQLBuilder().append("\n      AND ");
                }
                c.appendSQL(compiler);
            }
        }
        compiler.getSQLBuilder().append("\n");
    }

    private void orderBy(Compiler compiler) {
        if (!orderBys.isEmpty()) {
            compiler.getSQLBuilder().append("   ORDER BY ");
            Monoflop mf = Monoflop.create();
            for (Tuple<String, Boolean> e : orderBys) {
                if (mf.successiveCall()) {
                    compiler.getSQLBuilder().append(", ");
                }
                compiler.getSQLBuilder().append(e.getFirst());
                compiler.getSQLBuilder().append(e.getSecond() ? " ASC" : " DESC");
            }
            compiler.getSQLBuilder().append("\n");
        }
    }

    private void limit(Compiler compiler) {
        if (limit > 0 && db.hasCapability(Capability.LIMIT)) {
            if (start > 0) {
                compiler.getSQLBuilder().append("   LIMIT ").append(limit).append("\n");
            } else {
                compiler.getSQLBuilder().append("   LIMIT ").append(start).append(", ").append(limit).append("\n");
            }
        }
    }

    @Override
    public String toString() {
        return compile().toString();
    }
}
