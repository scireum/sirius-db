/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.SmartQuery;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An n-tuple of normal values that can be used in the OMA {@link sirius.db.jdbc.OMA#FILTERS filters}.
 * <p>
 * Use {@link Mapping} objects to indicate a column; every other object will be treated as an actual value.
 * Usage example:<br>
 * {@code
 * oma.select(Entity.class).where(OMA.FILTERS.gte(new RowValue(Entity.COLUMN1, Entity.COLUMN2), new RowValue(4, "Test")))
 * }
 * <br>will compile to<br>
 * {@code
 * SELECT * FROM Entity e1 WHERE (e1.column1, e2.column2) >= (4, "Test")
 * }
 */
public class RowValue {
    private final Object[] objects;

    /**
     * Construct a row value from its contents.
     *
     * @param objects an array of {@link Mapping mappings} and values.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("It is not much of a deal to use a varargs array")
    public RowValue(Object... objects) {
        this.objects = objects;
    }

    /**
     * Compiles the expression to the string representation.
     *
     * @param compiler the compiler to use.
     * @return a tuple containing the SQL string and the required parameters for the prepared statement.
     */
    public Tuple<String, List<Object>> compileExpression(SmartQuery.Compiler compiler) {
        List<String> sqlRepresentation = new ArrayList<>();
        List<Object> parameter = new ArrayList<>();
        for (Object object : objects) {
            if (object instanceof Mapping) {
                sqlRepresentation.add(compiler.translateColumnName((Mapping) object));
            } else {
                sqlRepresentation.add("?");
                parameter.add(object);
            }
        }
        if (objects.length == 1) {
            return Tuple.create(sqlRepresentation.get(0), parameter);
        }
        return Tuple.create("(" + Strings.join(sqlRepresentation, ", ") + ")", parameter);
    }

    @Override
    public String toString() {
        if (objects.length == 1) {
            return Objects.toString(objects[0]);
        }
        return "(" + Arrays.stream(objects).map(Objects::toString).collect(Collectors.joining(", ")) + ")";
    }
}
