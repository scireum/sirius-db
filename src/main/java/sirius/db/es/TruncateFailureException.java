/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;

import java.io.Serial;

/**
 * Signals that a {@linkplain ElasticQuery#tryTruncate() truncate} did not complete as expected due to unrecoverable errors
 * while processing the {@linkplain LowLevelClient#deleteByQuery(String, String, JSONObject) deleteByQuery} request.
 */
public class TruncateFailureException extends Exception {
    @Serial
    private static final long serialVersionUID = -2561233068012908165L;

    /**
     * Returns a new {@link TruncateFailureException} with a message containing the given response object.
     *
     * @param deleteByQueryResponse the response object of the failing {@linkplain LowLevelClient#deleteByQuery(String, String, JSONObject) deleteByQuery} request
     */
    public TruncateFailureException(JSONObject deleteByQueryResponse) {
        super("Truncate aborted due to unrecoverable error(s):\n" + deleteByQueryResponse.toJSONString());
    }
}
