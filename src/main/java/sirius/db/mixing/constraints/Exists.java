/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.constraints;

import com.google.common.collect.Lists;
import sirius.db.mixing.Column;
import sirius.db.mixing.Constraint;
import sirius.db.mixing.Entity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Schema;
import sirius.db.mixing.SmartQuery;
import sirius.kernel.di.std.Part;

import java.util.List;

/**
 * Generates an EXISTS subquery mapping the given field to the field of the queried entity.
 */
public class Exists extends Constraint {

    private Column outerColumn;
    private Column innerColumn;
    private List<Constraint> constraints = Lists.newArrayList();
    private Class<? extends Entity> other;
    private boolean not = false;

    private Exists() {
    }

    /**
     * Generates an NOT EXISTS clause: <tt>NOT EXISTS(SELECT * FROM other WHERE e.outerColumn = other.innerColumn</tt>.
     *
     * @param outerColumn the column of the entities being queried to match
     * @param other       the entity type to search in (which must not exist)
     * @param innerColumn the column within that inner entity type which must match the outer column
     * @return an exists constraint which can be filtered with additional constraints
     */
    public static Exists notMatchingIn(Column outerColumn, Class<? extends Entity> other, Column innerColumn) {
        Exists result = new Exists();
        result.outerColumn = outerColumn;
        result.innerColumn = innerColumn;
        result.other = other;
        result.not = true;
        return result;
    }

    /**
     * Generates an EXISTS clause: <tt>EXISTS(SELECT * FROM other WHERE e.outerColumn = other.innerColumn</tt>.
     *
     * @param outerColumn the column of the entities being queried to match
     * @param other       the entity type to search in (which must exist)
     * @param innerColumn the column within that inner entity type which must match the outer column
     * @return an exists constraint which can be filtered with additional constraints
     */
    public static Exists matchingIn(Column outerColumn, Class<? extends Entity> other, Column innerColumn) {
        Exists result = new Exists();
        result.outerColumn = outerColumn;
        result.innerColumn = innerColumn;
        result.other = other;
        return result;
    }

    /**
     * Adds an additional constraint to further filter the entities which must or must not exist.
     *
     * @param constraint the constaint on the inner entity type
     * @return the exists itself for fluent method calls
     */
    public Exists where(Constraint constraint) {
        constraints.add(constraint);

        return this;
    }

    @Override
    public boolean addsConstraint() {
        return true;
    }

    @Part
    private static Schema schema;

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        EntityDescriptor ed = schema.getDescriptor(other);
        String newAlias = compiler.generateTableAlias();
        if (not) {
            compiler.getWHEREBuilder().append("NOT ");
        }
        compiler.getWHEREBuilder()
                .append("EXISTS(SELECT * FROM ")
                .append(ed.getTableName())
                .append(" ")
                .append(newAlias)
                .append(" WHERE ")
                .append(compiler.translateColumnName(outerColumn))
                .append(" = ");
        String oldAlias = compiler.getDefaultAlias();
        try {
            compiler.setDefaultAlias(newAlias);
            compiler.getWHEREBuilder().append(compiler.translateColumnName(innerColumn));
            for (Constraint c : constraints) {
                if (c.addsConstraint()) {
                    compiler.getWHEREBuilder().append(" AND ");
                    c.appendSQL(compiler);
                }
            }
        } finally {
            compiler.setDefaultAlias(oldAlias);
        }
        compiler.getWHEREBuilder().append(")");
    }
}
