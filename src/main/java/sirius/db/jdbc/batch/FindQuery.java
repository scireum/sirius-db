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
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.Property;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a batch query which finds and entity in the database.
 * <p>
 * A query is created by enumerating which mappings to compare in order to identify the entity. The query
 * is then supplied with an example entity from which the search values are derived.
 *
 * @param <E> the generic type of entities to find with this query
 */
public class FindQuery<E extends SQLEntity> extends BatchQuery<E> {

    @Part
    private static Databases dbs;

    protected FindQuery(BatchContext context, Class<E> type, String[] mappings) {
        super(context, type, mappings);
    }

    /**
     * Tries to find a real database entity where the mappings to compare match the given example entity.
     *
     * @param example the example entity to search by
     * @return the matching entity wrapped as optional or an empty optional if no match was found
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public Optional<E> find(@Nonnull E example) {
        try {
            if (this.type == null) {
                this.type = (Class<E>) example.getClass();
            }

            Watch w = Watch.start();
            PreparedStatement stmt = prepareStmt();
            int i = 1;
            for (Property property : getProperties()) {
                stmt.setObject(i++, property.getValueForDatasource(OMA.class, example));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }

                return Optional.of((E) make(rs));
            } finally {
                avarage.addValue(w.elapsedMillis());
            }
        } catch (SQLException e) {
            context.safeClose();
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "A database error occured while executing a FindQuery for %s: %s (%s)",
                                    type.getName())
                            .handle();
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occured while executing a FindQuery for %s: %s (%s)",
                                                    type.getName())
                            .handle();
        }
    }

    private SQLEntity make(ResultSet rs) throws Exception {
        Set<String> columns = dbs.readColumns(rs);
        SQLEntity result = (SQLEntity) descriptor.make(OMA.class, null, key -> {
            String effeciveKey = key.toUpperCase();
            if (!columns.contains(effeciveKey)) {
                return null;
            }

            try {
                return Value.of(rs.getObject(effeciveKey));
            } catch (SQLException e) {
                throw Exceptions.handle(OMA.LOG, e);
            }
        });

        if (descriptor.isVersioned() && columns.contains(BaseMapper.VERSION.toUpperCase())) {
            result.setVersion(rs.getInt(BaseMapper.VERSION.toUpperCase()));
        }

        return result;
    }

    @Override
    protected void buildSQL() throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(getDescriptor().getRelationName());
        buildWhere(sql, false);

        createStmt(sql.toString(), false);
    }
}
