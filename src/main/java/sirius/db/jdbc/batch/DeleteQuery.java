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
import sirius.db.mixing.Property;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Represents a batch query which deletes one or more entities from the database.
 * <p>
 * A query is created by enumerating which mappings to compare in order to identify entities to delete. The query
 * is then supplied with an example entity from which the values are derived.
 *
 * @param <E> the generic type of entities to delete with this query
 */
public class DeleteQuery<E extends SQLEntity> extends BatchQuery<E> {

    protected DeleteQuery(BatchContext context, Class<E> type, List<Tuple<Operator, String>> filters) {
        super(context, type, filters);
    }

    /**
     * Deletes all entities where the compared mappings match the ones given in the <tt>example</tt>.
     *
     * @param example      the example entity used to determine which other entities to delete
     * @param invokeChecks <tt>true</tt> to signal that before- and after delete checks should be executed,
     *                     <tt>false</tt> otherwise
     * @param addBatch     determines if the query should be executed instantly (<tt>false</tt>) or added to the
     *                     batch update (<tt>true</tt>).
     */
    @SuppressWarnings("unchecked")
    public void delete(@Nonnull E example, boolean invokeChecks, boolean addBatch) {
        try {
            if (example.isNew()) {
                return;
            }

            if (this.type == null) {
                this.type = (Class<E>) example.getClass();
            }

            Watch w = Watch.start();
            if (invokeChecks) {
                getDescriptor().beforeDelete(example);
            }

            PreparedStatement stmt = prepareStmt();
            int i = 1;
            for (Tuple<Operator, Property> filter : getPropertyFilters()) {
                stmt.setObject(i++, filter.getSecond().getValueForDatasource(OMA.class, example));
            }

            if (descriptor.isVersioned()) {
                if (example.getVersion() == 0) {
                    throw Exceptions.handle()
                                    .to(OMA.LOG)
                                    .withSystemErrorMessage(
                                            "Cannot execute a DeleteQuery for the versioned entity %s without a version!",
                                            descriptor.getType())
                                    .handle();
                }
                stmt.setObject(i, example.getVersion());
            }

            if (addBatch) {
                addBatch();
            } else {
                stmt.executeUpdate();
                stmt.getConnection().commit();
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
                                    "A database error occurred while executing a DeleteQuery for %s: %s (%s)",
                                    type.getName())
                            .handle();
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occurred while executing a DeleteQuery for %s: %s (%s)",
                                                    type.getName())
                            .handle();
        }
    }

    @Override
    protected void buildSQL() throws SQLException {
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(getDescriptor().getRelationName());
        buildWhere(sql, true);

        createStmt(sql.toString(), false);
    }
}
