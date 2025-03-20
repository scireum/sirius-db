/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import sirius.db.DB;
import sirius.db.mixing.OptimisticLockException;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.health.Microtiming;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Internal fluent builder used to create, execute and handle requests via the given REST client.
 */
class RequestBuilder {

    private static final String PARAM_REASON = "reason";
    private static final String PARAM_TYPE = "type";
    private static final String PARAM_ROUTING = "routing";
    private static final String PARAM_IF_PRIMARY_TERM = "if_primary_term";
    private static final String PARAM_IF_SEQ_NO = "if_seq_no";
    private static final String PARAM_ERROR = "error";
    private static final int MAX_CONTENT_LONG_LENGTH = 1024;

    private String method;
    private RestClient restClient;
    private Map<String, String> params;
    private ObjectNode data;
    private String rawData;
    private HttpEntity responseEntity;
    private ObjectNode responseObject;
    private Function<ResponseException, HttpEntity> customExceptionHandler;

    @Part
    private static Elastic elastic;

    protected RequestBuilder(String method, RestClient restClient) {
        this.method = method;
        this.restClient = restClient;
    }

    protected RequestBuilder withParam(String param, Object value) {
        if (value != null) {
            if (params == null) {
                params = new HashMap<>();
            }

            params.put(param, String.valueOf(value));
        }

        return this;
    }

    protected RequestBuilder data(ObjectNode data) {
        this.data = data;
        return this;
    }

    protected RequestBuilder rawData(String data) {
        this.rawData = data;
        return this;
    }

    protected RequestBuilder withCustomErrorHandler(Function<ResponseException, HttpEntity> errorHandler) {
        this.customExceptionHandler = errorHandler;
        return this;
    }

    protected RequestBuilder routing(Object routing) {
        return withParam(PARAM_ROUTING, routing);
    }

    protected RequestBuilder primaryTerm(Object primaryTerm) {
        return withParam(PARAM_IF_PRIMARY_TERM, primaryTerm);
    }

    protected RequestBuilder seqNo(Object seqNo) {
        return withParam(PARAM_IF_SEQ_NO, seqNo);
    }

    protected RequestBuilder tryExecute(String uri) throws OptimisticLockException {
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> Strings.apply("Elastic: %s %s", method, uri), Duration.ofSeconds(30))) {
            Request request = setupRequest(uri);
            responseEntity = restClient.performRequest(request).getEntity();
            return this;
        } catch (ResponseException e) {
            return handleResponseException(e);
        } catch (IOException e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "An IO exception occurred when performing a request against elasticsearch: %s")
                            .handle();
        } finally {
            elastic.callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming("ELASTIC", method + ": " + uri);
            }
            if (w.elapsedMillis() > Elastic.getLogQueryThresholdMillis()) {
                elastic.numSlowQueries.inc();
                DB.SLOW_DB_LOG.INFO("A slow Elasticsearch query was executed (%s): %s\n%s\n%s",
                                    w.duration(),
                                    method + ": " + uri,
                                    Strings.limit(buildContent().orElse("no content"), MAX_CONTENT_LONG_LENGTH),
                                    ExecutionPoint.snapshot().toString());
            }
        }
    }

    private Request setupRequest(String uri) {
        if (Elastic.LOG.isFINE()) {
            Elastic.LOG.FINE("%s %s: %s",
                             method,
                             uri,
                             Strings.limit(buildContent().orElse("-"), MAX_CONTENT_LONG_LENGTH));
        }

        Request request = new Request(method, uri);
        request.addParameters(determineParams());
        NStringEntity requestContent =
                buildContent().map(content -> new NStringEntity(content, ContentType.APPLICATION_JSON)).orElse(null);
        request.setEntity(requestContent);
        return request;
    }

    private RequestBuilder handleResponseException(ResponseException e) throws OptimisticLockException {
        if (customExceptionHandler != null) {
            HttpEntity result = customExceptionHandler.apply(e);
            if (result != null) {
                responseEntity = result;
                return this;
            }
        }

        ObjectNode error = extractErrorJSON(e);
        if (e.getResponse().getStatusLine().getStatusCode() == 409) {
            throw new OptimisticLockException(error != null ?
                                              Json.tryValueString(error, PARAM_REASON).orElse(e.getMessage()) :
                                              e.getMessage(), e);
        }

        throw Exceptions.handle()
                        .to(Elastic.LOG)
                        .error(e)
                        .withSystemErrorMessage("Elasticsearch (%s) reported an error: %s (%s)",
                                                e.getResponse().getHost(),
                                                error != null ?
                                                Json.tryValueString(error, PARAM_REASON).orElse("unknown") :
                                                "unknown",
                                                error != null ?
                                                Json.tryValueString(error, PARAM_TYPE).orElse("-") :
                                                "-")
                        .handle();
    }

    private Map<String, String> determineParams() {
        return params == null ? Collections.emptyMap() : params;
    }

    private Optional<String> buildContent() {
        if (data != null) {
            return Optional.of(Json.write(data));
        }
        if (rawData != null) {
            return Optional.of(rawData);
        }

        return Optional.empty();
    }

    protected RequestBuilder execute(String uri) {
        try {
            return tryExecute(uri);
        } catch (OptimisticLockException e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage("An unexpected optimistic locking error occurred: %s")
                            .handle();
        }
    }

    protected void executeAsync(String uri,
                                @Nullable Consumer<Response> onSuccess,
                                @Nullable Consumer<HandledException> onFailure) {
        Request request = setupRequest(uri);

        restClient.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                if (onSuccess != null) {
                    onSuccess.accept(response);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                HandledException handledException = handleAsyncFailure(exception, uri);
                if (onFailure != null) {
                    onFailure.accept(handledException);
                }
            }
        });
    }

    private HandledException handleAsyncFailure(Exception exception, String uri) {
        return Exceptions.handle()
                         .to(Elastic.LOG)
                         .error(exception)
                         .withSystemErrorMessage("An unexpected error occurred when invoking '%s': %s (%s)", uri)
                         .handle();
    }

    protected ObjectNode extractErrorJSON(ResponseException e) {
        try {
            HttpEntity httpEntity = e.getResponse().getEntity();
            if (e.getResponse().getEntity().getContentLength() == 0) {
                return null;
            }
            ObjectNode response = Json.parseObject(EntityUtils.toString(httpEntity));
            return Json.getObject(response, PARAM_ERROR);
        } catch (IOException ex) {
            Exceptions.handle(Elastic.LOG, ex);
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage("Elasticsearch (%s) reported an error which cannot be unpacked: %s",
                                                    e.getResponse().getHost())
                            .handle();
        }
    }

    protected ObjectNode response() {
        try {
            if (responseObject == null) {
                if (responseEntity == null) {
                    throw new IllegalStateException("No response is available before making a request.");
                }

                responseObject = Json.parseObject(EntityUtils.toString(responseEntity));
            }

            return responseObject;
        } catch (IOException e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "An IO exception occurred when performing a request against elasticsearch: %s")
                            .handle();
        }
    }

    protected RequestBuilder toggle(String param, boolean toggle) {
        return withParam(param, String.valueOf(toggle));
    }

    protected RequestBuilder enable(String param, boolean flag) {
        if (!flag) {
            return this;
        }

        return withParam(param, "true");
    }

    protected RequestBuilder disable(String param, boolean flag) {
        if (flag) {
            return this;
        }

        return withParam(param, "false");
    }
}
