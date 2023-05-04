/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.kernel.commons.Json;
import sirius.kernel.health.Exceptions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents the result of a {@link BulkContext}.
 * <p>
 * For each invokation of {@link BulkContext#commit()}, a result is generated which may be used to handle errors
 * encountered while performing the queued commands.
 */
public class BulkResult {

    private static final String RESPONSE_KEY_ERROR = "error";
    private static final String RESPONSE_KEY_ITEMS = "items";
    private static final String RESPONSE_INDEX = "index";
    private static final String RESPONSE_TYPE = "type";
    private static final String RESPONSE_REASON = "reason";
    private static final String RESPONSE_CAUSED_BY = "caused_by";
    private static final String RESPONSE_KEY_ERRORS = "errors";

    private final ObjectNode bulkResponse;
    private Set<String> failedIds;
    private String failureMessage;

    /**
     * Creates a new response based on the given JSON returned by ES.
     *
     * @param bulkResponse the response returned by Elasticsearch or <tt>null</tt> to indicate an empty response. An
     *                     empty response may be created if {@link BulkContext#commit()} was called without any
     *                     queued commands
     */
    public BulkResult(ObjectNode bulkResponse) {
        this.bulkResponse = bulkResponse;
    }

    /**
     * Determines if the bulk update has been successfully executed.
     *
     * @return <tt>true</tt> if all commands were successfully executed
     */
    public boolean isSuccessful() {
        return bulkResponse == null || !bulkResponse.get(RESPONSE_KEY_ERRORS).asBoolean();
    }

    /**
     * Throws an appropriate exception if the bulk update encountered an error.
     *
     * @throws sirius.kernel.health.HandledException if the bulk update was not {@link #isSuccessful() successful}
     */
    public void throwFailures() {
        if (!isSuccessful()) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .withSystemErrorMessage("One or more commands failed within a bulk update: %s",
                                                    getFailureMessage())
                            .handle();
        }
    }

    /**
     * Returns all _id-fields of sub-requests which failed within this bulk request.
     *
     * @return a {@link Set} of _ids for which the bulk request failed.
     */
    public Set<String> getFailedIds() {
        if (isSuccessful()) {
            return Collections.emptySet();
        }

        if (failedIds == null) {
            digestResponse();
        }

        return Collections.unmodifiableSet(failedIds);
    }

    protected void digestResponse() {
        StringBuilder failureMessageBuilder = new StringBuilder();
        this.failedIds = new HashSet<>();
        ArrayNode items = Json.getArray(bulkResponse, RESPONSE_KEY_ITEMS);

        for (int i = 0; i < items.size(); i++) {
            ObjectNode current = getObject((ObjectNode) items.get(i));
            if (current.has(RESPONSE_KEY_ERROR)) {
                ObjectNode error = Json.getObject(current, RESPONSE_KEY_ERROR);
                failedIds.add(current.get(BulkContext.KEY_ID).asText());
                failureMessageBuilder.append("index: ")
                                     .append(error.get(RESPONSE_INDEX).asText())
                                     .append(" type: ")
                                     .append(error.get(RESPONSE_TYPE).asText())
                                     .append(" reason: ")
                                     .append(error.get(RESPONSE_REASON).asText());
                if (error.has(RESPONSE_CAUSED_BY)) {
                    failureMessageBuilder.append(" cause: ")
                                         .append(Json.getObject(error, RESPONSE_CAUSED_BY)
                                                     .get(RESPONSE_REASON)
                                                     .asText());
                }
                failureMessageBuilder.append("\n");
            }
        }

        this.failureMessage = failureMessageBuilder.toString();
    }

    private ObjectNode getObject(ObjectNode currentObject) {
        ObjectNode object = Json.getObject(currentObject, BulkContext.COMMAND_INDEX);
        if (object != null) {
            return object;
        }

        object = Json.getObject(currentObject, BulkContext.COMMAND_DELETE);
        if (object != null) {
            return object;
        }

        object = Json.getObject(currentObject, BulkContext.COMMAND_CREATE);
        if (object != null) {
            return object;
        }

        object = Json.getObject(currentObject, BulkContext.COMMAND_UPDATE);
        if (object != null) {
            return object;
        }

        throw Exceptions.handle().withSystemErrorMessage("Unknown object type within bulk-response!").handle();
    }

    /**
     * Returns the failure message for this bulk request.
     *
     * @return the failure message if errors occurred. Otherwise an empty string.
     */
    public String getFailureMessage() {
        if (failureMessage == null) {
            digestResponse();
        }

        return failureMessage;
    }
}
