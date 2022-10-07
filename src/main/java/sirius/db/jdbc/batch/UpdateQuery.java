/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.OMA;
import sirius.db.jdbc.Operator;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Property;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a batch query which updates an entity in the database.
 *
 * @param <E> the generic type of entities to update with this query
 */
public class UpdateQuery<E extends SQLEntity> extends BatchQuery<E> {

    private List<Property> propertiesToUpdate;

    protected UpdateQuery(BatchContext context, Class<E> type, List<Tuple<Operator, String>> filters) {
        super(context, type, filters);
    }

    /**
     * Specifies the list of mappings to update.
     * <p>
     * Note that this must be called once before this first entity is updated and cannot be changed later.
     *
     * @param mappingsToUpdate a list of mappings to update
     * @return the query itself for fluent method calls
     */
    public UpdateQuery<E> withUpdatedMappings(Mapping... mappingsToUpdate) {
        EntityDescriptor ed = getDescriptor();
        this.propertiesToUpdate = Arrays.stream(mappingsToUpdate).map(Mapping::getName).map(ed::getProperty).toList();
        return this;
    }

    protected List<Property> getPropertiesToUpdate() {
        if (propertiesToUpdate == null) {
            throw new IllegalStateException("No mappings to update were specified. Use '.withUpdatedMappings'!");
        }

        return Collections.unmodifiableList(propertiesToUpdate);
    }

    /**
     * Updates the given entity in the database by comparing the mappings to compare and updating the mappings to update.
     *
     * @param entity       the entity to update
     * @param invokeChecks determines if before- and after save checks should be performed (<tt>true</tt>)
     *                     or skipped (<tt>false</tt>)
     * @param addBatch     determines if the query should be executed instantly (<tt>false</tt>) or added to the
     *                     batch update (<tt>true</tt>).
     */
    @SuppressWarnings({"unchecked", "resource"})
    @Explain("We don't want to close the statement here - this is done in close() globally.")
    public void update(@Nonnull E entity, boolean invokeChecks, boolean addBatch) {
        try {
            if (this.type == null) {
                this.type = (Class<E>) entity.getClass();
            }

            Watch w = Watch.start();
            if (invokeChecks) {
                getDescriptor().beforeSave(entity);
            }

            PreparedStatement stmt = prepareAndFillForUpdate(entity);

            if (addBatch) {
                addBatch();
            } else {
                stmt.executeUpdate();
                stmt.getConnection().commit();
                average.addValue(w.elapsedMillis());
                if (descriptor.isVersioned()) {
                    entity.setVersion(entity.getVersion() + 1);
                }
            }

            if (invokeChecks) {
                getDescriptor().afterSave(entity);
            }
        } catch (SQLException e) {
            context.safeClose();
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "A database error occurred while executing an UpdateQuery for %s: %s (%s)",
                                    type.getName())
                            .handle();
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occurred while executing an UpdateQuery for %s: %s (%s)",
                                                    type.getName())
                            .handle();
        }
    }

    protected PreparedStatement prepareAndFillForUpdate(@Nonnull E entity) throws SQLException {
        PreparedStatement stmt = prepareStmt();
        int i = 1;
        for (Property property : getPropertiesToUpdate()) {
            stmt.setObject(i++, property.getValueForDatasource(OMA.class, entity));
        }

        if (descriptor.isVersioned()) {
            stmt.setObject(i++, entity.getVersion() + 1);
        }

        for (Tuple<Operator, Property> filter : getPropertyFilters()) {
            stmt.setObject(i++, filter.getSecond().getValueForDatasource(OMA.class, entity));
        }

        if (descriptor.isVersioned()) {
            if (entity.getVersion() == 0) {
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .withSystemErrorMessage("Cannot execute an UpdateQuery for the versioned entity"
                                                        + " %s without a version!", descriptor.getType())
                                .handle();
            }

            stmt.setObject(i, entity.getVersion());
        }
        return stmt;
    }

    @Override
    protected void buildSQL() throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(getDescriptor().getRelationName());

        Monoflop mf = Monoflop.create();
        sql.append(" SET ");
        for (Property p : getPropertiesToUpdate()) {
            if (mf.successiveCall()) {
                sql.append(", ");
            }
            sql.append(p.getPropertyName());
            sql.append(" = ?");
        }

        if (descriptor.isVersioned()) {
            if (mf.successiveCall()) {
                sql.append(", ");
            }
            sql.append(BaseMapper.VERSION);
            sql.append(" = ?");
        }

        buildWhere(sql, true);
        createStmt(sql.toString(), false);
    }
}
