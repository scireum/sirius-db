/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.suggest;

import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.Mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a result of a {@link SuggestionQuery}.
 */
public class SuggestionResult {

    private static final String KEY_SUGGEST = "suggest";
    private Map<String, List<TextPartSuggestion>> suggestions = new HashMap<>();

    /**
     * Generates a new result based on the given JSON response generated by Elasticsearch.
     *
     * @param json the JSON to parse
     */
    public SuggestionResult(JSONObject json) {
        JSONObject suggestionsObject = json.getJSONObject(KEY_SUGGEST);
        suggestionsObject.keySet().forEach(name -> {
            List<TextPartSuggestion> textPartSuggestions = suggestionsObject.getJSONArray(name)
                                                                            .stream()
                                                                            .map(option -> (JSONObject) option)
                                                                            .map(TextPartSuggestion::new)
                                                                            .collect(Collectors.toList());
            this.suggestions.put(name, textPartSuggestions);
        });
    }

    /**
     * Returns all suggestions for the given suggester.
     *
     * @param suggester the name of the suggester to fetch results for
     * @return a list of all suggestions
     */
    public List<TextPartSuggestion> getSuggestions(String suggester) {
        return Collections.unmodifiableList(suggestions.getOrDefault(suggester, Collections.emptyList()));
    }

    /**
     * Returns all suggestions for the given {@link Mapping field}.
     *
     * @param field the field to fetch suggestions for
     * @return a list of all suggestions
     */
    public List<TextPartSuggestion> getSuggestions(Mapping field) {
        return getSuggestions(field.toString());
    }

    /**
     * Directly returns all term suggestions for a given input term.
     * <p>
     * If the <tt>text</tt> provided is only a single term, directly
     * fetch the suggestions for this term.
     *
     * @param suggester the name of the suggester to fetch results for
     * @return a list of all suggestions
     */
    public List<TermSuggestion> getSingleTermSuggestions(String suggester) {
        List<TextPartSuggestion> textPartSuggestions = getSuggestions(suggester);
        if (textPartSuggestions.isEmpty()) {
            return Collections.emptyList();
        }

        return textPartSuggestions.get(0).getTermSuggestions();
    }

    /**
     * Directly returns all term suggestions for a given input term.
     * <p>
     * If the <tt>text</tt> provided is only a single term, directly
     * fetch the suggestions for this term.
     *
     * @param field the field to fetch suggestions for
     * @return a list of all suggestions
     */
    public List<TermSuggestion> getSingleTermSuggestions(Mapping field) {
        return getSingleTermSuggestions(field.toString());
    }
}
