/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.alibaba.fastjson2.JSONObject;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticEntity;
import sirius.db.es.LowLevelClient;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.kernel.di.std.Part;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a query which computes one or more suggestions.
 *
 * @param <E> the type of entities being queried
 */
public class SuggestionQuery<E extends ElasticEntity> {

    private static final String KEY_SUGGEST = "suggest";
    private final EntityDescriptor descriptor;
    private final LowLevelClient client;
    private List<SuggesterBuilder> suggesters = new ArrayList<>();

    @Part
    private static Elastic elastic;

    /**
     * Creates a new query.
     * <p>
     * Most probably this should be invoked via {@link Elastic#suggest(Class)}.
     *
     * @param descriptor the descriptor of the entities being queried
     * @param client     the client to use for the actual request
     */
    public SuggestionQuery(EntityDescriptor descriptor, LowLevelClient client) {
        this.descriptor = descriptor;
        this.client = client;
    }

    /**
     * Adds a term suggester for the given field using the given text.
     *
     * @param name  the name of the suggester (used to retrieve the results)
     * @param field the field to suggest terms for
     * @param text  the example term
     * @return the query itself for fluent method calls
     */
    public SuggestionQuery<E> withTermSuggester(String name, Mapping field, String text) {
        return withSuggester(new SuggesterBuilder(SuggesterBuilder.TYPE_TERM, name).on(field).forText(text));
    }

    /**
     * Adds a term suggester for the given field using the given text.
     *
     * @param field the field to suggest terms for, this will also be used a name for the generated suggestions.
     *              Use {@link SuggestionResult#getSingleTermSuggestions(Mapping)} to retrieve.
     * @param text  the example term
     * @return the query itself for fluent method calls
     */
    public SuggestionQuery<E> withTermSuggester(Mapping field, String text) {
        return withSuggester(new SuggesterBuilder(SuggesterBuilder.TYPE_TERM, field.toString()).on(field)
                                                                                               .forText(text));
    }

    /**
     * Adds a new suggester.
     *
     * @param suggester the suggester to add
     * @return the query itself for fluent method calls
     */
    public SuggestionQuery<E> withSuggester(SuggesterBuilder suggester) {
        suggesters.add(suggester);
        return this;
    }

    /**
     * Executes the suggestion query agains Elasticsearch and returns the results.
     *
     * @return the results as returned by ES
     */
    public SuggestionResult execute() {
        JSONObject response = client.search(elastic.determineReadAlias(descriptor), null, 0, 0, buildPayload());
        return new SuggestionResult(response);
    }

    private JSONObject buildPayload() {
        JSONObject payload = new JSONObject();
        JSONObject suggest = new JSONObject();
        suggesters.forEach(suggester -> suggest.put(suggester.getName(), suggester.build()));
        payload.put(KEY_SUGGEST, suggest);

        return payload;
    }
}
