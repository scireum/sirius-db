/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.SmartQuery;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;

import java.util.List;

/**
 * Represents a simple field operator as constraint.
 */
class FieldOperator extends SQLConstraint {

    private final RowValue lhs;
    private final RowValue rhs;
    private final String op;

    protected FieldOperator(RowValue lhs, String op, RowValue rhs) {
        this.lhs = lhs;
        this.op = op;
        this.rhs = rhs;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        Tuple<String, List<Object>> compiledLhs = lhs.compileExpression(compiler);
        Tuple<String, List<Object>> compiledRhs = rhs.compileExpression(compiler);
        compiler.getWHEREBuilder().append(Strings.join(" ", compiledLhs.getFirst(), op, compiledRhs.getFirst()));
        compiledLhs.getSecond().forEach(compiler::addParameter);
        compiledRhs.getSecond().forEach(compiler::addParameter);
    }

    @Override
    public void asString(StringBuilder builder) {
        builder.append(Strings.join(" ", lhs.toString(), op, rhs.toString()));
    }
}
