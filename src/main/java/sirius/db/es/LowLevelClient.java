/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import sirius.db.mixing.OptimisticLockException;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Provides a low-level client against Elasticsearch.
 * <p>
 * This is mainly used to build and execute HTTP requests and process their response.
 */
public class LowLevelClient {

    private static final String API_REINDEX = "/_reindex?wait_for_completion=false";
    private static final String API_TASK_INFO = "/_tasks/";
    private static final String API_ALIAS = "/_alias";
    private static final String API_ALIASES = "/_aliases";
    private static final String API_SEARCH = "/_search";
    private static final String API_DELETE_BY_QUERY = "/_delete_by_query";
    private static final String API_PREFIX_DOC = "/_doc/";
    private static final String API_REFRESH = "/_refresh";
    private static final String API_SETTINGS = "/_settings";
    private static final String API_CLUSTER_HEALTH = "/_cluster/health";
    private static final String API_STATS = "/_stats";
    private static final String API_MAPPING = "/_mapping";
    private static final String API_BULK = "_bulk";

    private static final String PARAM_INDEX = "index";
    private static final String PARAM_ALIAS = "alias";
    private static final String PARAM_ACTIONS = "actions";
    private static final String PARAM_NUMBER_OF_SHARDS = "number_of_shards";
    private static final String PARAM_NUMBER_OF_REPLICAS = "number_of_replicas";
    private static final String PARAM_SETTINGS = "settings";

    private static final String PARAM_REFRESH = "refresh";
    private static final String ACTON_ADD = "add";
    private static final String ACTION_REMOVE = "remove";

    private static final String REQUEST_METHOD_HEAD = "HEAD";
    private static final String REQUEST_METHOD_GET = "GET";
    private static final String REQUEST_METHOD_POST = "POST";
    private static final String REQUEST_METHOD_PUT = "PUT";
    private static final String REQUEST_METHOD_DELETE = "DELETE";

    private final RestClient restClient;

    /**
     * Enumerates possible values for {@link #PARAM_REFRESH} in {@link #bulkWithRefresh(List, Refresh)}.
     */
    public enum Refresh {
        /**
         * Force a refresh of the affected shards.
         */
        TRUE,

        /**
         * Do nothing with the affected shards.
         */
        FALSE,

        /**
         * Force and await the refresh of the affected shards.
         */
        WAIT_FOR
    }

    /**
     * Creates a new client based on the given REST client which handles load balancing and connection management.
     *
     * @param restClient the underlying REST client to use
     */
    public LowLevelClient(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * Returns the underlying REST client.
     *
     * @return the underlying REST client
     */
    public RestClient getRestClient() {
        return restClient;
    }

    /**
     * Builds a GET request.
     *
     * @return a request builder used to execute the request
     */
    protected RequestBuilder performGet() {
        return new RequestBuilder(REQUEST_METHOD_GET, getRestClient());
    }

    /**
     * Builds a POST request.
     *
     * @return a request builder used to execute the request
     */
    protected RequestBuilder performPost() {
        return new RequestBuilder(REQUEST_METHOD_POST, getRestClient());
    }

    /**
     * Builds a PUT request.
     *
     * @return a request builder used to execute the request
     */
    protected RequestBuilder performPut() {
        return new RequestBuilder(REQUEST_METHOD_PUT, getRestClient());
    }

    /**
     * Builds a DELETE request.
     *
     * @return a request builder used to execute the request
     */
    protected RequestBuilder performDelete() {
        return new RequestBuilder(REQUEST_METHOD_DELETE, getRestClient());
    }

    /**
     * Tells Elasticsearch to create or update the given document with the given data.
     *
     * @param index       the target index
     * @param id          the ID to use
     * @param routing     the routing to use
     * @param primaryTerm the primaryTerm to use for optimistic locking during the update
     * @param seqNo       the seqNo to use for optimistic locking during the update
     * @param data        the actual payload to store
     * @return the response of the call
     * @throws OptimisticLockException in case of an optimistic locking error (wrong version provided)
     */
    public JSONObject index(String index,
                            String id,
                            @Nullable String routing,
                            @Nullable Long primaryTerm,
                            @Nullable Long seqNo,
                            JSONObject data) throws OptimisticLockException {
        return performPut().routing(routing)
                           .primaryTerm(primaryTerm)
                           .seqNo(seqNo)
                           .data(data)
                           .tryExecute(index + API_PREFIX_DOC + id)
                           .response();
    }

    /**
     * Performs a lookup for the given document.
     *
     * @param index      the index to search in
     * @param id         the ID to search by
     * @param routing    the routing value to use
     * @param withSource <tt>true</tt> to also load the <tt>_source</tt> of the document, <tt>false</tt> otherwise
     * @return the response of the call
     */
    public JSONObject get(String index, String id, @Nullable String routing, boolean withSource) {
        return performGet().withCustomErrorHandler(this::handleNotFoundAsResponse)
                           .routing(routing)
                           .disable("_source", withSource)
                           .execute(index + API_PREFIX_DOC + id)
                           .response();
    }

    /**
     * Deletes the given document.
     *
     * @param index       the target index
     * @param id          the ID to use
     * @param routing     the routing to use
     * @param primaryTerm the primaryTerm to use for optimistic locking during the deletion
     * @param seqNo       the seqNo to use for optimistic locking during the deletion
     * @return the response of the call
     * @throws OptimisticLockException in case of an optimistic locking error (wrong version provided)
     */
    public JSONObject delete(String index, String id, String routing, Long primaryTerm, Long seqNo)
            throws OptimisticLockException {
        return performDelete().withCustomErrorHandler(this::handleNotFoundAsResponse)
                              .routing(routing)
                              .primaryTerm(primaryTerm)
                              .seqNo(seqNo)
                              .tryExecute(index + API_PREFIX_DOC + id)
                              .response();
    }

    protected HttpEntity handleNotFoundAsResponse(ResponseException e) {
        if (e.getResponse().getStatusLine().getStatusCode() == 404) {
            return e.getResponse().getEntity();
        } else {
            return null;
        }
    }

    /**
     * Deletes all documents matched by the given query.
     *
     * @param alias   the alias which determines the indices to search in
     * @param routing the routing to use
     * @param query   the query to execute
     * @return the response of the call
     * @throws OptimisticLockException if one of the documents was modified during the runtime of the deletion query
     */
    public JSONObject deleteByQuery(String alias, @Nullable String routing, JSONObject query)
            throws OptimisticLockException {
        return performPost().routing(routing).data(query).tryExecute(alias + API_DELETE_BY_QUERY).response();
    }

    /**
     * Executes a search.
     *
     * @param alias   the alias which determines the indices to search in
     * @param routing the routing to use
     * @param from    the number of items to skip
     * @param size    the maximal result length
     * @param query   the query to execute
     * @return the response of the call
     */
    public JSONObject search(String alias, @Nullable String routing, int from, int size, JSONObject query) {
        return performGet().routing(routing)
                           .withParam("size", size)
                           .withParam("from", from)
                           .data(query)
                           .execute(alias + API_SEARCH)
                           .response();
    }

    /**
     * Executes a reindex request.
     * <p>
     * Note that this starts a re-index request and returns the created task id.
     *
     * @param sourceIndexName      the source index to read from
     * @param destinationIndexName the name of the index in which the documents should be re-indexed
     * @return the ID of the background task within Elasticsearch
     */
    public String startReindex(String sourceIndexName, String destinationIndexName) {
        JSONObject response = performPost().data(new JSONObject().fluentPut("source",
                                                                            new JSONObject().fluentPut(PARAM_INDEX,
                                                                                                       sourceIndexName))
                                                                 .fluentPut("dest",
                                                                            new JSONObject().fluentPut(PARAM_INDEX,
                                                                                                       destinationIndexName)))
                                           .execute(API_REINDEX)
                                           .response();

        return response.getString("task");
    }

    /**
     * Determines if the task with the given ID is still active.
     *
     * @param taskId the task ID to check
     * @return <tt>true</tt> if the task is alive and active, <tt>false</tt> otherwise
     */
    public boolean checkTaskActivity(String taskId) {
        JSONObject response =
                performGet().execute(API_TASK_INFO + Strings.urlEncode(taskId)).withCustomErrorHandler(ex -> {
                    try {
                        if (ex.getResponse().getStatusLine().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                            return new StringEntity("{ notFound: true }");
                        } else {
                            return null;
                        }
                    } catch (UnsupportedEncodingException e) {
                        throw Exceptions.handle(e);
                    }
                }).response();

        return !response.containsKey("notFound") && !response.getBooleanValue("completed");
    }

    /**
     * Adds an alias to a given index.
     *
     * @param indexName the name of the index which should be aliased
     * @param alias     the alias to apply
     * @return the response of the call
     */
    public JSONObject addAlias(String indexName, String alias) {
        return performPut().execute("/" + indexName + API_ALIAS + "/" + alias).response();
    }

    /**
     * Checks whether an index for this alias exists.
     *
     * @param alias the given alias
     * @return true if the index exists
     */
    public boolean aliasExists(String alias) {
        try {
            return restClient.performRequest(new Request(REQUEST_METHOD_HEAD, API_ALIAS + "/" + alias))
                             .getStatusLine()
                             .getStatusCode() == 200;
        } catch (ResponseException e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occurred when checking for alias '%s': %s (%s)", alias)
                            .handle();
        } catch (IOException e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage("An IO error occurred when checking for index '%s': %s (%s)", alias)
                            .handle();
        }
    }

    /**
     * Returns the actual index for the given alias
     *
     * @param aliasName the alias the resolve
     * @return the index name to which this alias points or an empty optional if the alias is unknown
     * @throws HandledException if the alias points to more than one index as this pattern is unused by
     *                          out schema evolution tools (which always redirects an index access via an alias).
     */
    public Optional<String> resolveIndexForAlias(String aliasName) {
        List<String> indexNames = new ArrayList<>();
        performGet().withCustomErrorHandler(error -> {
            // We cannot use handleNotFoundAsResponse here, as we have to return a truly empty
            // JSON object, otherwise the error and its status code will be reported as indices...
            if (error.getResponse().getStatusLine().getStatusCode() == 404) {
                return new StringEntity("{}", StandardCharsets.UTF_8);
            } else {
                return null;
            }
        }).execute(API_ALIAS + "/" + aliasName).response().forEach((indexName, info) -> indexNames.add(indexName));

        if (indexNames.isEmpty()) {
            return Optional.empty();
        }

        if (indexNames.size() > 1) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .withSystemErrorMessage("The alias %s points to more than one index: %s",
                                                    aliasName,
                                                    Strings.join(indexNames, ", "))
                            .handle();
        }

        return Optional.of(indexNames.get(0));
    }

    /**
     * Performs a move operation of the given alias to point to the given destination.
     * <p>
     * This will also atomically remove the previous destination if the alias did already exist.
     * </p>
     *
     * @param alias       the alias to create or update
     * @param destination the index to which the alias should point
     * @return the response of the call
     */
    public JSONObject createOrMoveAlias(String alias, String destination) {
        if (!indexExists(destination)) {
            throw Exceptions.handle()
                            .withSystemErrorMessage("There exists no index with name '%s'", destination)
                            .handle();
        }

        JSONArray actions = new JSONArray();

        JSONObject add = new JSONObject().fluentPut(PARAM_INDEX, destination).fluentPut(PARAM_ALIAS, alias);
        actions.add(new JSONObject().fluentPut(ACTON_ADD, add));

        resolveIndexForAlias(alias).ifPresent(oldIndex -> {
            JSONObject remove = new JSONObject().fluentPut(PARAM_INDEX, oldIndex).fluentPut(PARAM_ALIAS, alias);
            actions.add(new JSONObject().fluentPut(ACTION_REMOVE, remove));
        });

        return performPost().data(new JSONObject().fluentPut(PARAM_ACTIONS, actions)).execute(API_ALIASES).response();
    }

    /**
     * Creates a scroll search.
     *
     * @param alias        the alias which determines the indices to search in
     * @param routing      the routing to use
     * @param from         the number of items to skip
     * @param sizePerShard the maximal number of results per shard
     * @param ttlSeconds   the ttl of the scroll cursor in seconds
     * @param query        the query to execute
     * @return the response of the call
     * @deprecated The scroll API is deprecated by elastic. Try to use the search_after API with a point in time instead.
     */
    @Deprecated(since = "2023/04/28")
    public JSONObject createScroll(String alias,
                                   String routing,
                                   int from,
                                   int sizePerShard,
                                   int ttlSeconds,
                                   JSONObject query) {
        return performGet().routing(routing)
                           .withParam("size", sizePerShard)
                           .withParam("from", from)
                           .withParam("scroll", ttlSeconds + "s")
                           .data(query)
                           .execute(alias + API_SEARCH)
                           .response();
    }

    /**
     * Continues a scroll query.
     *
     * @param ttlSeconds the ttl of the scroll cursor in seconds
     * @param scrollId   the id of the scroll cursor
     * @return the response of the call
     * @deprecated The scroll API is deprecated by elastic. Try to use the search_after API with a point in time instead.
     */
    @Deprecated(since = "2023/04/28")
    public JSONObject continueScroll(int ttlSeconds, String scrollId) {
        return performGet().data(new JSONObject().fluentPut("scroll", ttlSeconds + "s")
                                                 .fluentPut("scroll_id", scrollId))
                           .execute("/_search/scroll")
                           .response();
    }

    /**
     * Closes a scroll query.
     *
     * @param scrollId the id of the scroll cursor
     * @return the response of the call
     * @deprecated The scroll API is deprecated by elastic. Try to use the search_after API with a point in time instead.
     */
    @Deprecated(since = "2023/04/28")
    public JSONObject closeScroll(String scrollId) {
        return performDelete().data(new JSONObject().fluentPut("scroll_id", scrollId))
                              .execute("/_search/scroll")
                              .response();
    }

    /**
     * Creates a point in time (PIT) for the given alias and routing.
     *
     * @param alias     the alias to create the PIT for
     * @param routing   the routing to create the PIT for
     * @param keepAlive how long the PIT should live (initially)
     * @return the id of the PIT
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/api-conventions.html#time-units">Elastic time units</a>
     */
    public String createPit(String alias, String routing, String keepAlive) {
        return performPost().routing(routing)
                            .withParam("keep_alive", keepAlive)
                            .execute(alias + "/_pit")
                            .response()
                            .getString("id");
    }

    /**
     * Closes the given PIT manually.
     * <p>
     * This allows ES to free the needed resources earlier than the PIT would expire.
     *
     * @param pit the id of the PIT to close
     */
    public void closePit(String pit) {
        performDelete().data(new JSONObject().fluentPut("id", pit)).execute("/_pit");
    }

    /**
     * Determines if a given query has at least one result.
     *
     * @param alias   the alias which determines the indices to search in
     * @param routing the routing to use
     * @param query   the query to execute
     * @return the response of the call
     */
    public JSONObject exists(String alias, String routing, JSONObject query) {
        return performGet().routing(routing)
                           .withParam("size", 0)
                           .withParam("terminate_after", 1)
                           .data(query)
                           .execute(alias + API_SEARCH)
                           .response();
    }

    /**
     * Determines the number of hits for a given query.
     *
     * @param alias   the alias which determines the indices to search in
     * @param routing the routing to use
     * @param query   the query to execute
     * @return the response of the call
     */
    public JSONObject count(String alias, String routing, JSONObject query) {
        return performGet().routing(routing).data(query).execute(alias + "/_count").response();
    }

    /**
     * Executes a list of bulk statements.
     *
     * @param bulkData the statements to execute.
     * @return the response of the call
     * @see BulkContext
     */
    public JSONObject bulk(List<JSONObject> bulkData) {
        return bulkWithRefresh(bulkData, Refresh.FALSE);
    }

    /**
     * Executes a list of bulk statements with the given refresh setting.
     *
     * @param bulkData the statements to execute
     * @param refresh  the refresh mode to use
     * @return the response of the call
     * @see BulkContext
     */
    @SuppressWarnings("squid:S1612")
    @Explain("Due to method overloading the compiler cannot deduce which method to pick")
    public JSONObject bulkWithRefresh(List<JSONObject> bulkData, Refresh refresh) {
        return performPost().withParam(PARAM_REFRESH, refresh.name().toLowerCase())
                            .rawData(bulkData.stream().map(obj -> obj.toJSONString()).collect(Collectors.joining("\n"))
                                     + "\n")
                            .execute(API_BULK)
                            .response();
    }

    /**
     * Creates the given index.
     *
     * @param index              the name of the index
     * @param numberOfShards     the number of shards to use
     * @param numberOfReplicas   the number of replicas per shard
     * @param settingsCustomizer a callback which may further extend the settings object passed to Elasticsearch
     * @return the response of the call
     */
    public JSONObject createIndex(String index,
                                  int numberOfShards,
                                  int numberOfReplicas,
                                  @Nullable Consumer<JSONObject> settingsCustomizer) {
        JSONObject indexObj = new JSONObject().fluentPut(PARAM_NUMBER_OF_SHARDS, numberOfShards)
                                              .fluentPut(PARAM_NUMBER_OF_REPLICAS, numberOfReplicas);
        JSONObject settingsObj = new JSONObject().fluentPut(PARAM_INDEX, indexObj);
        if (settingsCustomizer != null) {
            settingsCustomizer.accept(settingsObj);
        }
        JSONObject input = new JSONObject().fluentPut(PARAM_SETTINGS, settingsObj);
        return performPut().data(input).execute(index).response();
    }

    /**
     * Creates the given mapping.
     *
     * @param index the name of the index
     * @param data  the mapping to create
     * @return the response of the call
     */
    public JSONObject putMapping(String index, JSONObject data) {
        return performPut().data(data).execute(index + API_MAPPING).response();
    }

    /**
     * Determines if the given index exists.
     *
     * @param index the name of the index
     * @return <tt>true</tt> if the index exists, <tt>false</tt> otherwise
     */
    public boolean indexExists(String index) {
        try {
            return restClient.performRequest(new Request(REQUEST_METHOD_HEAD, index)).getStatusLine().getStatusCode()
                   == HttpURLConnection.HTTP_OK;
        } catch (ResponseException e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage("An error occurred when checking for index '%s': %s (%s)", index)
                            .handle();
        } catch (IOException e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage("An IO error occurred when checking for index '%s': %s (%s)", index)
                            .handle();
        }
    }

    /**
     * Allows to explicitly refresh an index, making all operations performed since the last refresh available for search.
     *
     * @param index the index which should be refreshed
     */
    public void refresh(String index) {
        performPost().execute(index + API_REFRESH).response();
    }

    /**
     * Entirely wipes the given index and all its data.
     *
     * @param index the index to delete
     */
    public void deleteIndex(String index) {
        performDelete().execute(index).response();
    }

    /**
     * Fetches the settings for the given index.
     *
     * @param index the index to fetch the settings for
     * @return a JSON object as returned by <tt>/index/_settings</tt>
     */
    public JSONObject indexSettings(String index) {
        return performGet().execute(index + API_SETTINGS).response();
    }

    /**
     * Fetches the cluster health.
     *
     * @return a JSON object as returned by <tt>/_cluster/health</tt>
     */
    public JSONObject clusterHealth() {
        return performGet().execute(API_CLUSTER_HEALTH).response();
    }

    /**
     * Fetches statistics for all indices.
     *
     * @return a JSON object as returned by <tt>/_stats</tt>
     */
    public JSONObject indexStats() {
        return performGet().execute(API_STATS).response();
    }
}
