/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.constraints;

import sirius.mixing.Constraint;
import sirius.mixing.SmartQuery;

/**
 * Created by aha on 27.04.15.
 */
public class FieldEqual extends Constraint {

    private String field;
    private Object value;

    public FieldEqual(String field, Object value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public boolean addsConstraint() {
        return value != null;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        compiler.getSQLBuilder().append(field).append(" = ?");
        compiler.addParameter(value);
    }
}
