/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;
import sirius.kernel.settings.PortMapper;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Provides a client to access a qdrant vector database.
 */
public class QdrantDatabase {

    private static final String QDRANT_DEFAULT_HOST = "qdrant";
    private static final int QDRANT_DEFAULT_PORT = 6333;
    public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(60);
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    public static final int HTTP_STATUS_OK = 200;
    public static final String URI_PREFIX_COLLECTIONS = "/collections/";

    /**
     * Specifies the similarity functions supported by qdrant.
     */
    public enum Similarity {
        DOT("Dot"), COSINE("Cosine"), EUCLID("Euclid");

        private final String name;

        Similarity(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    enum Method {
        GET, PUT, POST, DELETE
    }

    private final Tuple<String, Integer> effectiveHostAndPort;
    private final HttpClient client;

    protected QdrantDatabase(String hostname) {
        this.effectiveHostAndPort = PortMapper.mapPort(QDRANT_DEFAULT_HOST, hostname, QDRANT_DEFAULT_PORT);
        this.client = HttpClient.newBuilder()
                                .version(HttpClient.Version.HTTP_1_1)
                                .followRedirects(HttpClient.Redirect.NEVER)
                                .connectTimeout(CONNECT_TIMEOUT)
                                .build();
    }

    protected HttpResponse<String> executeRaw(Method method, String uri, @Nullable JSONObject input) {
        Watch watch = Watch.start();
        try (Operation operation = new Operation(() -> "qdrant: " + method.name() + ": " + uri,
                                                 DEFAULT_OPERATION_TIMEOUT)) {
            HttpRequest.Builder requestBuilder = getHeader(uri);
            if (input != null) {
                requestBuilder.method(method.name(), HttpRequest.BodyPublishers.ofString(input.toJSONString()));
            } else {
                requestBuilder.method(method.name(), HttpRequest.BodyPublishers.noBody());
            }

            return client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw Exceptions.handle()
                            .withSystemErrorMessage("Got interrupted while executing a qdrant request: %s (%s)")
                            .error(error)
                            .handle();
        } catch (Exception error) {
            throw Exceptions.handle()
                            .withSystemErrorMessage("Failed to execute qdrant request: %s (%s)")
                            .error(error)
                            .handle();
        } finally {
            Qdrant.callDuration.addValue(watch.elapsedMillis());
            watch.submitMicroTiming("QDRANT", uri);
        }
    }

    @SuppressWarnings("HttpUrlsUsage")
    private HttpRequest.Builder getHeader(String uri) throws URISyntaxException {
        return HttpRequest.newBuilder(new URI("http://"
                                              + effectiveHostAndPort.getFirst()
                                              + ":"
                                              + effectiveHostAndPort.getSecond()
                                              + uri)).header(HEADER_CONTENT_TYPE, CONTENT_TYPE_APPLICATION_JSON);
    }

    protected JSONObject execute(Method method, String uri, @Nullable JSONObject input) {
        HttpResponse<String> response = executeRaw(method, uri, input);
        if (response.statusCode() != HTTP_STATUS_OK) {
            throw Exceptions.handle()
                            .withSystemErrorMessage("Failed to execute Qdrant request. Received: %s and: %s",
                                                    response.statusCode(),
                                                    response.body())
                            .handle();
        }
        return JSON.parseObject(response.body());
    }

    /**
     * Creates a new collection with the given name, dimensions and similarity function.
     *
     * @param collection name of the collection
     * @param dimensions number of dimensions in the stored vectors
     * @param similarity the similarity function to use
     */
    public void createCollection(String collection, int dimensions, Similarity similarity) {
        execute(Method.PUT,
                URI_PREFIX_COLLECTIONS + collection,
                new JSONObject().fluentPut("name", collection)
                                .fluentPut("vectors",
                                           new JSONObject().fluentPut("size", dimensions)
                                                           .fluentPut("distance", similarity.getName())));
    }

    /**
     * Deletes the collection with the given name.
     *
     * @param collection the name of the collection to delete
     */
    public void deleteCollection(String collection) {
        execute(Method.DELETE, URI_PREFIX_COLLECTIONS + collection, null);
    }

    /**
     * Determines if a collection with the given name exists.
     *
     * @param collection the name of the collection to check
     * @return <tt>true</tt> if a collection with the given name exists, <tt>false</tt> otherwise
     */
    public boolean collectionExists(String collection) {
        return executeRaw(Method.GET, URI_PREFIX_COLLECTIONS + collection, null).statusCode() == HTTP_STATUS_OK;
    }

    /**
     * Ensures that a collection with the given name exists.
     *
     * @param collection name of the collection
     * @param dimensions number of dimensions in the stored vectors (if case the collection is created)
     * @param similarity the similarity function to use (if case the collection is created)
     */
    public void ensureCollectionExists(String collection, int dimensions, Similarity similarity) {
        if (!collectionExists(collection)) {
            createCollection(collection, dimensions, similarity);
        }
    }

    /**
     * Upserts the given points into the given collection.
     * <p>
     * Upsert indicates, that if a point with the same id already exists, it will be replaced, otherwise it will be
     * added.
     *
     * @param collection the name of the collection to upsert the points into
     * @param points     the points to upsert
     */
    public void upsert(String collection, List<Point> points) {
        JSONArray pointArray = new JSONArray();
        points.stream().map(Point::toJson).forEach(pointArray::add);

        execute(Method.PUT,
                URI_PREFIX_COLLECTIONS + collection + "/points?wait=true",
                new JSONObject().fluentPut("points", pointArray));
    }

    /**
     * Counts the number of points in the given collection.
     *
     * @param collection the name of the collection to count the points in
     * @param exact      <tt>true</tt> to count the exact number of points, <tt>false</tt> to count the approximate number
     * @return the number of points in the given collection
     */
    public long countPoints(String collection, boolean exact) {
        return execute(Method.POST,
                       URI_PREFIX_COLLECTIONS + collection + "/points/count",
                       new JSONObject().fluentPut("exact", exact)).getJSONObject("result").getLong("count");
    }

    /**
     * Deletes the points with the given ids from the given collection.
     *
     * @param collection the name of the collection to delete the points from
     * @param pointIds   the ids of the points to delete
     */
    public void deletePoints(String collection, List<String> pointIds) {
        execute(Method.POST,
                URI_PREFIX_COLLECTIONS + collection + "/points/delete",
                new JSONObject().fluentPut("points", pointIds));
    }

    /**
     * Creates a new search for the given collection and vector.
     *
     * @param collection the name of the collection to search in
     * @param vector     the vector to search for
     * @return a new search query
     */
    public Search query(String collection, float[] vector) {
        return new Search(this, collection, vector);
    }
}
