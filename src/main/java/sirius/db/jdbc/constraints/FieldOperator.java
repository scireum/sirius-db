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

    private final RowValue leftHandSide;
    private final RowValue rightHandSide;
    private final String operator;

    protected FieldOperator(RowValue leftHandSide, String operator, RowValue rightHandSide) {
        this.leftHandSide = leftHandSide;
        this.operator = operator;
        this.rightHandSide = rightHandSide;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        Tuple<String, List<Object>> compiledLhs = leftHandSide.compileExpression(compiler);
        Tuple<String, List<Object>> compiledRhs = rightHandSide.compileExpression(compiler);
        compiler.getWHEREBuilder().append(Strings.join(" ", compiledLhs.getFirst(), operator, compiledRhs.getFirst()));
        compiledLhs.getSecond().forEach(compiler::addParameter);
        compiledRhs.getSecond().forEach(compiler::addParameter);
    }

    @Override
    public void asString(StringBuilder builder) {
        builder.append(Strings.join(" ", leftHandSide.toString(), operator, rightHandSide.toString()));
    }
}
