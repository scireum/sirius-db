/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.BaseSQLQuery;
import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.OMA;
import sirius.db.jdbc.Row;
import sirius.db.mixing.EntityDescriptor;
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

public class InsertQuery<E extends SQLEntity> extends BatchQuery<E> {

    private boolean fetchId;

    protected InsertQuery(BatchContext context, Class<E> type, boolean fetchId, String[] mappings) {
        super(context, type, mappings);
        this.fetchId = fetchId;
    }

    @SuppressWarnings("unchecked")
    public void insert(@Nonnull E entity, boolean invokeChecks, boolean addBatch) {
        try {
            if (this.type == null) {
                this.type = (Class<E>) entity.getClass();
            }

            Watch w = Watch.start();
            PreparedStatement stmt = prepareStmt();
            int i = 1;
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
                if (fetchId) {
                    Row keys = BaseSQLQuery.fetchGeneratedKeys(stmt);
                    OMA.loadCreatedId(entity, keys);
                }
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
                            .withSystemErrorMessage(
                                    "A database error occured while executing an InsertQuery for %s: %s (%s)",
                                    type.getName())
                            .handle();
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occured while executing an InsertQuery for %s: %s (%s)",
                                                    type.getName())
                            .handle();
        }
    }

    @Override
    protected List<Property> getProperties() {
        if (properties == null) {
            EntityDescriptor ed = getDescriptor();
            if (mappings.length == 0) {
                properties = ed.getProperties()
                               .stream()
                               .filter(p -> !SQLEntity.ID.getName().equals(p.getName()))
                               .collect(Collectors.toList());
            } else {
                properties = Arrays.stream(mappings).map(ed::getProperty).collect(Collectors.toList());
            }
        }

        return properties;
    }

    @Override
    protected void buildSQL() throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        StringBuilder values = new StringBuilder(" VALUES(");
        sql.append(getDescriptor().getRelationName());
        sql.append(" (");
        Monoflop mf = Monoflop.create();
        for (Property p : getProperties()) {
            if (mf.successiveCall()) {
                sql.append(", ");
                values.append(", ?");
            } else {
                values.append("?");
            }
            sql.append(p.getPropertyName());
        }
        sql.append(")");
        values.append(")");
        sql.append(values);
        createStmt(sql.toString(), fetchId);
    }
}
