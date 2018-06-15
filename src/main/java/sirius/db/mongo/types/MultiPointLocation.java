/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.types;

import sirius.db.mixing.types.SafeList;
import sirius.kernel.commons.Tuple;

/**
 * Represents a list of 2D locations which can be stored in MongoDB for geo queries.
 */
public class MultiPointLocation extends SafeList<Tuple<Double, Double>> {

    @Override
    protected boolean valueNeedsCopy() {
        return true;
    }

    @Override
    protected Tuple<Double, Double> copyValue(Tuple<Double, Double> value) {
        return Tuple.create(value.getFirst(), value.getSecond());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Tuple<Double, Double> location : data()) {
            sb.append(location.getFirst());
            sb.append(",");
            sb.append(location.getSecond());
            sb.append("\n");
        }

        return sb.toString();
    }
}
