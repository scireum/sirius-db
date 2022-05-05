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
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An n-tuple of normal values that can be used in the OMA {@link sirius.db.jdbc.OMA#FILTERS filters}.
 * <p>
 * Use {@link Mapping} objects to indicate a column, every other object will be treated as an actual value.
 * Usage example:<br>
 * {@code
 * oma.select(Entity.class)
 * .where(OMA.FILTERS.gte(new CompoundValue(Entity.COLUMN1, Entity.COLUMN2), new CompoundValue(4, "Test")))
 * }
 * <br>will compile to<br>
 * {@code
 * SELECT * FROM Entity e1 WHERE (e1.column1, e2.column2) >= (4, "Test")
 * }
 */
public class CompoundValue {

    private final List<Object> components = new ArrayList<>();

    /**
     * Creates an empty compound value.
     */
    public CompoundValue() {
    }

    /**
     * Creates a new compound value with the given initial component.
     *
     * @param initialComponent the initial component to add to the compound value
     */
    public CompoundValue(Object initialComponent) {
        components.add(initialComponent);
    }

    /**
     * Adds a component to the compound value.
     *
     * @param value the value to add
     * @return the compound value itself for fluent method calls
     */
    public CompoundValue addComponent(Object value) {
        this.components.add(value);
        return this;
    }

    /**
     * Compiles the expression to the string representation.
     *
     * @param compiler the compiler to use.
     * @return a tuple containing the SQL string and the required parameters for the prepared statement.
     */
    public Tuple<String, List<Object>> compileExpression(SmartQuery.Compiler compiler) {
        List<String> sqlRepresentation = new ArrayList<>(components.size());
        List<Object> parameters = new ArrayList<>(components.size());
        for (Object component : components) {
            if (component instanceof Mapping mapping) {
                sqlRepresentation.add(compiler.translateColumnName(mapping));
            } else {
                sqlRepresentation.add("?");
                parameters.add(component);
            }
        }
        if (components.size() == 1) {
            return Tuple.create(sqlRepresentation.get(0), parameters);
        }
        return Tuple.create("(" + Strings.join(sqlRepresentation, ", ") + ")", parameters);
    }

    @Override
    public String toString() {
        if (components.size() == 1) {
            return Objects.toString(components.get(0));
        }
        return "(" + components.stream().map(Objects::toString).collect(Collectors.joining(", ")) + ")";
    }
}
