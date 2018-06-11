/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;

import java.util.Optional;
import java.util.function.Function;

public interface ESPropertyInfo {

    default void transferOption(String key,
                                Function<IndexMode, ESOption> mapper,
                                ESOption defaultValue,
                                JSONObject mapping) {
        ESOption option =
                Optional.ofNullable(getClass().getAnnotation(IndexMode.class)).map(mapper).orElse(ESOption.ES_DEFAULT);

        if (option != ESOption.ES_DEFAULT) {
            mapping.put(key, option.toString());
        }
    }

    void describeProperty(JSONObject description);
}
