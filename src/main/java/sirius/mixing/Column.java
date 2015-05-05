/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

/**
 * Created by aha on 04.05.15.
 */
public class Column {
    private final String name;
    private final Column parent;

    private Column(String name, Column parent) {
        this.name = name;
        this.parent = parent;
    }

    public static Column named(String name) {
        return new Column(name, null);
    }

    public Column inner(Column inner) {
        return new Column(name + "_" + inner.name, null);
    }

    public Column join(Column joinColumn) {
        return new Column(joinColumn.name, this);
    }

    public String getName() {
        return name;
    }

    public Column getParent() {
        return parent;
    }

    @Override
    public String toString() {
        if (parent != null) {
            return parent.toString() + "." + name;
        } else {
            return name;
        }
    }
}
