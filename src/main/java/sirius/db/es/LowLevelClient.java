/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import sirius.db.mixing.OptimisticLockException;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Exceptions;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class LowLevelClient {

    private RestClient restClient;

    public LowLevelClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public RestClient getRestClient() {
        return restClient;
    }

    public RequestBuilder performGet() {
        return new RequestBuilder("GET", getRestClient());
    }

    public RequestBuilder performPost() {
        return new RequestBuilder("POST", getRestClient());
    }

    public RequestBuilder performPut() {
        return new RequestBuilder("PUT", getRestClient());
    }

    public RequestBuilder performDelete() {
        return new RequestBuilder("DELETE", getRestClient());
    }

    public JSONObject index(String index, String type, String id, String routing, Integer version, JSONObject data)
            throws OptimisticLockException {
        return performPut().routing(routing)
                           .version(version)
                           .data(data)
                           .tryExecute(index + "/" + type + "/" + id)
                           .response();
    }

    public JSONObject get(String index, String type, String id, String routing, boolean withSource) {
        return performGet().withCustomErrorHandler(this::handleNotFoundAsResponse)
                           .routing(routing)
                           .disable("_source", withSource)
                           .execute(index + "/" + type + "/" + id)
                           .response();
    }

    protected HttpEntity handleNotFoundAsResponse(ResponseException e) {
        if (e.getResponse().getStatusLine().getStatusCode() == 404) {
            return e.getResponse().getEntity();
        } else {
            return null;
        }
    }

    public JSONObject search(List<String> indices, String type, String routing, int from, int size, JSONObject query) {
        return performGet().routing(routing)
                           .withParam("size", size)
                           .withParam("from", from)
                           .data(query)
                           .execute(Strings.join(indices, ",") + "/" + type + "/_search")
                           .response();
    }

    public JSONObject createScroll(List<String> indices,
                                   String type,
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
                           .execute(Strings.join(indices, ",") + "/" + type + "/_search")
                           .response();
    }

    public JSONObject continueScroll(int ttlSeconds, String scrollId) {
        return performGet().data(new JSONObject().fluentPut("scroll", ttlSeconds + "s")
                                                 .fluentPut("scroll_id", scrollId))
                           .execute("/_search/scroll")
                           .response();
    }

    public JSONObject closeScroll(String scrollId) {
        return performDelete().data(new JSONObject().fluentPut("scroll_id", scrollId))
                              .execute("/_search/scroll")
                              .response();
    }

    public JSONObject exists(List<String> indices, String type, String routing, JSONObject query) {
        return performGet().routing(routing)
                           .withParam("size", 0)
                           .withParam("terminate_after", 1)
                           .data(query)
                           .execute(Strings.join(indices, ",") + "/" + type + "/_search")
                           .response();
    }

    public JSONObject count(List<String> indices, String type, String routing, JSONObject query) {
        return performGet().routing(routing)
                           .data(query)
                           .execute(Strings.join(indices, ",") + "/" + type + "/_count")
                           .response();
    }

    public JSONObject delete(String index, String type, String id, String routing, Integer version)
            throws OptimisticLockException {
        return performDelete().routing(routing).version(version).tryExecute(index + "/" + type + "/" + id).response();
    }

    public JSONObject deleteByQuery(String index,
                                    String type,
                                    String routing,
                                    boolean proceedOnConflict,
                                    JSONObject query) {
        return performPost().routing(routing)
                            .withParam("conflicts", proceedOnConflict ? "proceed" : null)
                            .execute(index + "/" + type + "/_delete_by_query")
                            .data(new JSONObject().fluentPut("query", query))
                            .response();
    }

    public JSONObject bulk(List<JSONObject> bulkData) {
        return performPost().rawData(bulkData.stream().map(obj -> obj.toJSONString()).collect(Collectors.joining("\n"))
                                     + "\n", "application/x-ndjson").execute("_bulk").response();
    }

    public JSONObject createIndex(String index, int numberOfShards, int numberOfReplicas) {
        JSONObject indexObj = new JSONObject().fluentPut("number_of_shards", numberOfShards)
                                              .fluentPut("number_of_replicas", numberOfReplicas);
        JSONObject settingsObj = new JSONObject().fluentPut("index", indexObj);
        JSONObject input = new JSONObject().fluentPut("settings", settingsObj);
        return performPut().data(input).execute(index).response();
    }

    public JSONObject putMapping(String index, String type, JSONObject data) {
        return performPut().data(data).execute(index + "/_mapping/" + type).response();
    }

    public boolean indexExists(String index) {
        try {
            return restClient.performRequest("HEAD", index).getStatusLine().getStatusCode() == 200;
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
}
