/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Tuple;

import java.util.List;
import java.util.function.Function;

/**
 * Created by aha on 29.11.14.
 */
public class SmartQuery<E extends Entity> extends BaseQuery<E> {
    private String[] fields;
    private List<Tuple<String, Boolean>> orderBys = Lists.newArrayList();
    private List<Tuple<String, String[]>> joinFetches = Lists.newArrayList();

    protected SmartQuery(Class<E> type) {
        super(type);
    }

    @Override
    public void iterate(Function<E, Boolean> handler) {

    }

    @Override
    public SmartQuery<E> start(int start) {
        return (SmartQuery<E>)super.start(start);
    }

    @Override
    public SmartQuery<E> limit(int limit) {
        return (SmartQuery<E>)super.limit(limit);
    }

    public SmartQuery<E> orderAsc(String field) {
        orderBys.add(Tuple.create(field, true));
        return this;
    }

    public SmartQuery<E> orderDesc(String field) {
        orderBys.add(Tuple.create(field, false));
        return this;
    }

    public SmartQuery<E> fields(String... fields) {
        this.fields = fields;
        return this;
    }

    public SmartQuery<E> joinFetch(String relation, String... fields) {
        joinFetches.add(Tuple.create(relation, fields));
        return this;
    }

    public long count() {
        return 0;
    }


}
