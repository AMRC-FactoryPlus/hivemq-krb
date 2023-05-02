/* Factory+ Java client library.
 * HTTP client.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus.http;

import java.net.*;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.ServiceConfigurationError;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.ietf.jgss.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.hc.client5.http.HttpResponseException;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.client5.http.fluent.Response;
import org.apache.hc.client5.http.impl.cache.CacheConfig;
import org.apache.hc.client5.http.impl.cache.CachingHttpClients;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.net.URIBuilder;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.async.methods.SimpleRequestProducer;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.cache.CachingHttpAsyncClients;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;

import org.json.*;

import io.reactivex.rxjava3.core.Single;

import uk.co.amrc.factoryplus.*;
import uk.co.amrc.factoryplus.gss.*;

public class FPHttpClient {
    private static final Logger log = LoggerFactory.getLogger(FPHttpClient.class);

    private FPServiceClient fplus;
    private CloseableHttpClient http_client;
    private CloseableHttpAsyncClient async_client;
    private String service_auth;
    private RequestCache<URI, String> tokens;

    public FPHttpClient (FPServiceClient fplus)
    {
        this.fplus = fplus;

        String srv_user = fplus.getConf("service_username");
        String srv_pass = fplus.getConf("service_password");
        service_auth = "Basic " 
            + Base64.getEncoder().encodeToString(
                (srv_user + ":" + srv_pass).getBytes());

        CacheConfig cache_config = CacheConfig.custom()
            .setSharedCache(false)
            .build();
        http_client = CachingHttpClients.custom()
            .setCacheConfig(cache_config)
            .build();

        tokens = new RequestCache<URI, String>(this::tokenFor);

        final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
            .setSoTimeout(Timeout.ofSeconds(5))
            .build();

        async_client = CachingHttpAsyncClients.custom()
            .setIOReactorConfig(ioReactorConfig)
            .build();
    }

    public void start ()
    {
        FPThreadUtil.logId("Running async HTTP client");
        async_client.start();
    }

    public FPHttpRequest request (UUID service, String method)
    {
        return new FPHttpRequest(this, service, method);
    }

    private SimpleHttpRequest makeRequest (String method, URI base, String path,
        String auth, String creds)
    {
        URI uri = base.resolve(path);

        log.info("Making request {} {}", method, uri);
        var end = creds.length() > 5 ? 5 : creds.length();
        log.info("Using auth {} {}...", auth, creds.substring(0, end));

        var req = new SimpleHttpRequest(method, uri);
        req.setHeader("Authorization", auth + " " + creds);

        return req;
    }

    public Single<Object> execute (FPHttpRequest fpr)
    {
        FPThreadUtil.logId("execute called");
        return fplus.discovery()
            .get(fpr.service)
            .flatMap(srv_base -> tokens
                .get(srv_base)
                .map(tok -> Pair.of(srv_base, tok)))
            .flatMap(srv -> {
                /* Insert Princess Bride reference here... */
                var base = srv.getLeft();
                var tok = srv.getRight();
                FPThreadUtil.logId("creating request");

                var req = makeRequest(fpr.method, base, fpr.path, "Bearer", tok);
                if (fpr.body != null) {
                    req.setBody(fpr.body.toString(),
                        ContentType.APPLICATION_JSON);
                }
                return fetch(req);
            })
            .doOnSuccess(o -> FPThreadUtil.logId("execute result"));
    }

    public Single<String> tokenFor (URI service)
    {
        return Single.fromCallable(() -> {
                FPThreadUtil.logId("getting gss context");
                return fplus.gssClient()
                    .createContextHB("HTTP@" + service.getHost())
                    .orElseThrow(() -> new Exception("Can't get GSS context"));
            })
            .map(ctx -> {
                FPThreadUtil.logId("getting gss token");
                return ctx.initSecContext(new byte[0], 0, 0);
            })
            .map(tok -> Base64.getEncoder().encodeToString(tok))
            .map(tok -> makeRequest("POST", service, "token", "Negotiate", tok))
            .subscribeOn(fplus.getScheduler())
            .flatMap(req -> fetch(req))
            /* fetch moves calls below here to the http thread pool */
            .cast(JSONObject.class)
            .map(o -> o.getString("token"));
    }

    private Single<Object> fetch (SimpleHttpRequest req)
    {
        FPThreadUtil.logId("fetch called");
        return Single.<SimpleHttpResponse>create(obs ->
                async_client.execute(req,
                    new FutureCallback<SimpleHttpResponse>() {
                        public void completed (SimpleHttpResponse res) {
                            FPThreadUtil.logId("fetch success");
                            obs.onSuccess(res);
                        }

                        public void failed (Exception ex) {
                            FPThreadUtil.logId("fetch failure");
                            obs.onError(ex);
                        }

                        public void cancelled () {
                            obs.onError(new Exception("HTTP future cancelled"));
                        }
                    }))
            .doOnSuccess(res -> {
                FPThreadUtil.logId("handling fetch response");
                log.info("Fetch response {}: {}", req.getUri(), res.getCode());
            })
            .map(res -> res.getBodyText())
            .map(json -> new JSONTokener(json).nextValue());
    }
}
