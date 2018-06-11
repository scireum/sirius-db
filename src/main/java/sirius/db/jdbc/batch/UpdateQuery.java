/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.OMA;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Property;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateQuery<E extends SQLEntity> extends BatchQuery<E> {

    private String[] mappingsToUpdate;
    private List<Property> propertiesToUpdate;

    protected UpdateQuery(BatchContext context, Class<E> type, String[] mappings) {
        super(context, type, mappings);
    }

    public UpdateQuery<E> withUpdatedMappings(Mapping... mappingsToUpdate) {
        this.mappingsToUpdate = BatchContext.simplifyMappings(mappingsToUpdate);
        return this;
    }

    public UpdateQuery<E> withUpdatedMappings(String... mappingsToUpdate) {
        this.mappingsToUpdate = mappingsToUpdate;
        return this;
    }

    protected List<Property> getPropertiesToUpdate() {
        if (propertiesToUpdate == null) {
            if (mappingsToUpdate == null) {
                throw new IllegalStateException("No mappings to update were specified. Use '.withUpdatedMappings'!");
            }
            EntityDescriptor ed = getDescriptor();
            propertiesToUpdate = Arrays.stream(mappingsToUpdate).map(ed::getProperty).collect(Collectors.toList());
        }

        return propertiesToUpdate;
    }

    @SuppressWarnings("unchecked")
    public void update(@Nonnull E entity, boolean invokeChecks, boolean addBatch) {
        try {
            if (this.type == null) {
                this.type = (Class<E>) entity.getClass();
            }

            Watch w = Watch.start();
            PreparedStatement stmt = prepareStmt();
            int i = 1;
            for (Property property : getPropertiesToUpdate()) {
                stmt.setObject(i++, property.getValueForDatasource(entity));
            }
            for (Property property : getProperties()) {
                stmt.setObject(i++, property.getValueForDatasource(entity));
            }

            if (invokeChecks) {
                getDescriptor().beforeSave(entity);
            }

            if (addBatch) {
                addBatch();
            } else {
                stmt.executeUpdate();
                avarage.addValue(w.elapsedMillis());
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
                                    "A database error occured while executing an UpdateQuery for %s: %s (%s)",
                                    type.getName())
                            .handle();
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occured while executing an UpdateQuery for %s: %s (%s)",
                                                    type.getName())
                            .handle();
        }
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
        sql.append(" WHERE ");

        mf = Monoflop.create();
        for (Property p : getProperties()) {
            if (mf.successiveCall()) {
                sql.append("AND ");
            }
            sql.append(p.getPropertyName());
            sql.append(" = ?");
        }
        createStmt(sql.toString(), false);
    }
}
