/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.Databases;
import sirius.db.jdbc.OMA;
import sirius.db.jdbc.Row;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Property;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a batch query which inserts an entity into the database.
 *
 * @param <E> the generic type of entities to insert with this query
 */
public class InsertQuery<E extends SQLEntity> extends BatchQuery<E> {

    private boolean fetchId;
    private List<Property> propertiesToUpdate;

    @Part
    private static Databases dbs;

    protected InsertQuery(BatchContext context, Class<E> type, boolean fetchId, List<String> mappingsToUpdate) {
        super(context, type, Collections.emptyList());
        this.fetchId = fetchId;
        EntityDescriptor ed = getDescriptor();
        if (mappingsToUpdate.isEmpty()) {
            this.propertiesToUpdate = ed.getProperties()
                                        .stream()
                                        .filter(p -> !SQLEntity.ID.getName().equals(p.getName()))
                                        .collect(Collectors.toList());
        } else {
            this.propertiesToUpdate = mappingsToUpdate.stream().map(ed::getProperty).collect(Collectors.toList());
        }
    }

    /**
     * Inserts an entity into the database.
     * <p>
     * Note that on the occurence of a {@link SQLException}, the underlying statement and connection are closed,
     * as the might have become inconsistent. On the next call a new connection and statement is created transparently.
     * <p>
     * If exceptions are excepted, e.g. {@link SQLIntegrityConstraintViolationException} for optimistic locking
     * patterns, {@link #optimisticInsert(SQLEntity, boolean, boolean)} can be used.
     *
     * @param entity       the entity to insert
     * @param invokeChecks determines if before- and after save checks should be performed (<tt>true</tt>) or
     *                     skipped (<tt>false</tt>)
     * @param addBatch     determines if the query should be executed instantly (<tt>false</tt>) or added to the
     *                     batch update (<tt>true</tt>).
     * @throws sirius.kernel.health.HandledException in case of a database error or a general exception
     */
    public void insert(@Nonnull E entity, boolean invokeChecks, boolean addBatch) {
        try {
            optimisticInsert(entity, invokeChecks, addBatch);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "An integrity check failed while executing an InsertQuery for %s: %s (%s)",
                                    type.getName())
                            .handle();
        }
    }

    /**
     * Inserts an entity into the database, just like {@link #insert(SQLEntity, boolean, boolean)}, but differs in
     * handling integrity exceptions.
     * <p>
     * If a {@link SQLIntegrityConstraintViolationException} is thrown (i.e. a unique constraint is violated), this
     * will be handed to the caller without handling the exception an, more essentially, without closing the underlying
     * statement and exception. All other database and general errors are handled and thrown as
     * {@link HandledException}.
     *
     * @param entity       the entity to insert
     * @param invokeChecks determines if before- and after save checks should be performed (<tt>true</tt>) or
     *                     skipped (<tt>false</tt>)
     * @param addBatch     determines if the query should be executed instantly (<tt>false</tt>) or added to the
     *                     batch update (<tt>true</tt>).
     * @throws SQLIntegrityConstraintViolationException when reported by the underlying database
     * @throws sirius.kernel.health.HandledException    in case of a database error or a general exception
     */
    @SuppressWarnings("unchecked")
    public void optimisticInsert(@Nonnull E entity, boolean invokeChecks, boolean addBatch)
            throws SQLIntegrityConstraintViolationException {
        try {
            if (this.type == null) {
                this.type = (Class<E>) entity.getClass();
            }

            Watch w = Watch.start();
            if (invokeChecks) {
                getDescriptor().beforeSave(entity);
            }

            PreparedStatement stmt = prepareStmt();
            int i = 1;
            for (Property property : propertiesToUpdate) {
                stmt.setObject(i++, property.getValueForDatasource(OMA.class, entity));
            }

            if (descriptor.isVersioned()) {
                stmt.setObject(i, 1);
            }

            if (addBatch) {
                addBatch();
            } else {
                stmt.executeUpdate();
                stmt.getConnection().commit();
                if (fetchId) {
                    Row keys = dbs.fetchGeneratedKeys(stmt);
                    OMA.loadCreatedId(entity, keys);
                }
                entity.setVersion(1);
            }

            if (invokeChecks) {
                getDescriptor().afterSave(entity);
            }
            if (!addBatch) {
                avarage.addValue(w.elapsedMillis());
            }
        } catch (SQLException e) {
            context.safeClose();
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("A database error occured while executing an InsertQuery"
                                                    + " for %s: %s (%s)", type.getName())
                            .handle();
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occured while executing an InsertQuery"
                                                    + " for %s: %s (%s)", type.getName())
                            .handle();
        }
    }

    @Override
    protected void buildSQL() throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        StringBuilder values = new StringBuilder(" VALUES(");
        sql.append(getDescriptor().getRelationName());
        sql.append(" (");
        Monoflop mf = Monoflop.create();
        for (Property p : propertiesToUpdate) {
            if (mf.successiveCall()) {
                sql.append(", ");
                values.append(", ?");
            } else {
                values.append("?");
            }
            sql.append(p.getPropertyName());
        }

        if (descriptor.isVersioned()) {
            if (mf.successiveCall()) {
                sql.append(", ");
                values.append(", ?");
            } else {
                values.append("?");
            }
            sql.append(BaseMapper.VERSION);
        }

        sql.append(")");
        values.append(")");
        sql.append(values);
        createStmt(sql.toString(), fetchId);
    }
}
