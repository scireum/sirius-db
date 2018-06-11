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
import sirius.db.mixing.Property;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DeleteQuery<E extends SQLEntity> extends BatchQuery<E> {

    protected DeleteQuery(BatchContext context, Class<E> type, String[] mappings) {
        super(context, type, mappings);
    }

    @SuppressWarnings("unchecked")
    public void delete(@Nonnull E example, boolean invokeChecks, boolean addBatch) {
        try {
            if (this.type == null) {
                this.type = (Class<E>) example.getClass();
            }

            Watch w = Watch.start();
            PreparedStatement stmt = prepareStmt();
            int i = 1;
            for (Property property : getProperties()) {
                stmt.setObject(i++, property.getValueForDatasource(example));
            }

            if (invokeChecks) {
                getDescriptor().beforeDelete(example);
            }

            if (addBatch) {
                addBatch();
            } else {
                stmt.executeUpdate();
                avarage.addValue(w.elapsedMillis());
            }

            if (invokeChecks) {
                getDescriptor().afterDelete(example);
            }
        } catch (SQLException e) {
            context.safeClose();
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "A database error occured while executing a DeleteQuery for %s: %s (%s)",
                                    type.getName())
                            .handle();
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occured while executing a DeleteQuery for %s: %s (%s)",
                                                    type.getName())
                            .handle();
        }
    }

    @Override
    protected void buildSQL() throws SQLException {
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(getDescriptor().getRelationName());
        sql.append(" WHERE ");
        Monoflop mf = Monoflop.create();
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
