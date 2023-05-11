/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;
import sirius.kernel.settings.PortMapper;

import javax.annotation.Nullable;
import java.net.HttpURLConnection;
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
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(60);
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    protected static final String URI_PREFIX_COLLECTIONS = "/collections/";

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

    protected enum Method {
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

    protected HttpResponse<String> executeRaw(Method method, String uri, @Nullable ObjectNode input) {
        Watch watch = Watch.start();
        try (Operation operation = new Operation(() -> "qdrant: " + method.name() + ": " + uri,
                                                 DEFAULT_OPERATION_TIMEOUT)) {
            HttpRequest.Builder requestBuilder = getHeader(uri);
            if (input != null) {
                requestBuilder.method(method.name(), HttpRequest.BodyPublishers.ofString(Json.write(input)));
            } else {
                requestBuilder.method(method.name(), HttpRequest.BodyPublishers.noBody());
            }

            return client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw Exceptions.handle()
                            .to(Qdrant.LOG)
                            .withSystemErrorMessage("Got interrupted while executing a qdrant request: %s (%s)")
                            .error(error)
                            .handle();
        } catch (Exception error) {
            throw Exceptions.handle()
                            .to(Qdrant.LOG)
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

    protected ObjectNode execute(Method method, String uri, @Nullable ObjectNode input) {
        HttpResponse<String> response = executeRaw(method, uri, input);
        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw Exceptions.handle()
                            .withSystemErrorMessage("Failed to execute Qdrant request. Received: %s and: %s",
                                                    response.statusCode(),
                                                    response.body())
                            .handle();
        }
        return Json.parseObject(response.body());
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
                Json.createObject()
                    .put("name", collection)
                    .set("vectors", Json.createObject().put("size", dimensions).put("distance", similarity.getName())));
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
        return executeRaw(Method.GET, URI_PREFIX_COLLECTIONS + collection, null).statusCode()
               == HttpURLConnection.HTTP_OK;
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
        ArrayNode pointArray = Json.createArray();
        points.stream().map(Point::toJson).forEach(pointArray::add);

        execute(Method.PUT,
                URI_PREFIX_COLLECTIONS + collection + "/points?wait=true",
                Json.createObject().set("points", pointArray));
    }

    /**
     * Counts the number of points in the given collection.
     *
     * @param collection the name of the collection to count the points in
     * @param exact      <tt>true</tt> to count the exact number of points, <tt>false</tt> to count the approximate number
     * @return the number of points in the given collection
     */
    public long countPoints(String collection, boolean exact) {
        ObjectNode response = execute(Method.POST,
                                      URI_PREFIX_COLLECTIONS + collection + "/points/count",
                                      Json.createObject().put("exact", exact));
        return Json.tryGetAt(response, Json.createPointer("result", "count")).map(JsonNode::asLong).orElse(0L);
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
                Json.createObject().putPOJO("points", pointIds));
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
