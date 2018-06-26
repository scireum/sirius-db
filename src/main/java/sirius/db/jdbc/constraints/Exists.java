/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import com.google.common.collect.Lists;
import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.SmartQuery;
import sirius.db.jdbc.TranslationState;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixing;
import sirius.kernel.di.std.Part;

import java.util.List;

/**
 * Generates an EXISTS subquery mapping the given field to the field of the queried entity.
 */
public class Exists extends SQLConstraint {

    private Mapping outerColumn;
    private Mapping innerColumn;
    private List<SQLConstraint> constraints = Lists.newArrayList();
    private Class<? extends SQLEntity> other;

    @Part
    private static Mixing mixing;

    protected Exists(Mapping outerColumn, Class<? extends SQLEntity> other, Mapping innerColumn) {
        this.outerColumn = outerColumn;
        this.other = other;
        this.innerColumn = innerColumn;
    }

    /**
     * Adds an additional constraint to further filter the entities which must or must not exist.
     *
     * @param constraint the constaint on the inner entity type
     * @return the exists itself for fluent method calls
     */
    public Exists where(SQLConstraint constraint) {
        if (constraint != null) {
            constraints.add(constraint);
        }

        return this;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        // Determines the target descriptor and a new alias
        EntityDescriptor ed = mixing.getDescriptor(other);
        String newAlias = compiler.generateTableAlias();

        // Creates a backup of the current WHERE string builder
        StringBuilder originalWHEREBuilder = compiler.getWHEREBuilder();
        compiler.setWHEREBuilder(new StringBuilder());

        // Applies the main constraint and switches the underlying JOIN state just in time.
        compiler.getWHEREBuilder().append(" WHERE ").append(compiler.translateColumnName(outerColumn)).append(" = ");
        TranslationState state = compiler.captureAndReplaceTranslationState(newAlias, ed);
        compiler.getWHEREBuilder().append(compiler.translateColumnName(innerColumn));

        // Applies additional constraints...
        for (SQLConstraint c : constraints) {
            compiler.getWHEREBuilder().append(" AND ");
            c.appendSQL(compiler);
        }

        // Generates the effective EXISTS constraint, restores the original WHERE builder
        // and appends the EXISTS to it...
        StringBuilder existsBuilder = new StringBuilder();
        existsBuilder.append("EXISTS(SELECT * FROM ").append(ed.getRelationName()).append(" ").append(newAlias);
        existsBuilder.append(compiler.getJoins());

        compiler.restoreTranslationState(state);
        StringBuilder existsWHEREBuilder = compiler.getWHEREBuilder();
        compiler.setWHEREBuilder(originalWHEREBuilder);
        compiler.getWHEREBuilder().append(existsBuilder).append(existsWHEREBuilder).append(")");
    }

    @Override
    public void asString(StringBuilder builder) {
        //TODO
    }
}
