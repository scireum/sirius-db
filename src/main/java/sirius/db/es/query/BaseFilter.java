/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.query;

import com.alibaba.fastjson.JSONObject;

public abstract class BaseFilter implements Filter {

    @Override
    public String toString() {
        JSONObject jsonObject = toJSON();
        if (jsonObject == null) {
            return "<disabled>";
        }

        return jsonObject.toString();
    }
}
