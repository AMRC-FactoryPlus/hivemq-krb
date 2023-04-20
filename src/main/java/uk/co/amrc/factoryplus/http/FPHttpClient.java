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

import org.apache.hc.client5.http.HttpResponseException;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.client5.http.fluent.Response;
import org.apache.hc.client5.http.impl.cache.CacheConfig;
import org.apache.hc.client5.http.impl.cache.CachingHttpClients;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.net.URIBuilder;

import org.json.*;

import io.reactivex.rxjava3.core.Single;

import uk.co.amrc.factoryplus.*;
import uk.co.amrc.factoryplus.gss.*;

public class FPHttpClient {
    private static final Logger log = LoggerFactory.getLogger(FPHttpClient.class);

    private FPServiceClient fplus;
    private CloseableHttpClient http_client;
    private String service_auth;
    private TokenCache tokens;

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

        tokens = new TokenCache(this::tokenFor);
    }

    public FPHttpRequest request (UUID service, String method)
    {
        return new FPHttpRequest(this, service, method);
    }

    /* Java's URI class doesn't resolve relative URIs properly unless
     * there is an explicit path component. */
    private URI fixPath (URI uri)
    {
        return uri.getPath().length() == 0 ? uri.resolve("/") : uri;
    }

    private Request makeRequest (String method, URI base, String path,
        String auth)
    {
        URI uri = base.resolve(path);
        Request req = Request.create(method, uri)
            .setHeader("Authorization", auth);

        log.info("Making request {} {}", method, uri);
        log.info("Using auth {}", auth);

        return req;
    }

    public Single<Object> execute (FPHttpRequest fpr)
    {
        Set<URI> urls = fplus.discovery().lookup(fpr.service);
        if (urls.isEmpty())
            return Single.<Object>error(
                new Exception("Cannot find service URL"));

        /* Just take the first (only) for now. */
        URI srv_base = fixPath(urls.iterator().next());
        log.info("Resolved {} to {}", fpr.service, srv_base);

        return tokens.get(srv_base)
            .flatMap(tok -> {
                FPThreadUtil.logId("creating request");

                var auth = "Bearer " + tok;
                var req = makeRequest(fpr.method, srv_base, fpr.path, auth);
                if (fpr.body != null) {
                    req.bodyString(fpr.body.toString(),
                        ContentType.APPLICATION_JSON);
                }
                return Single.fromCallable(() -> {
                        return fetch(req)
                            .orElseThrow(() ->
                                new Exception("fetch failed!"));
                    })
                    .subscribeOn(fplus.getScheduler());
            })
            .doOnSuccess(o -> FPThreadUtil.logId("execute result"));
    }

    public Single<String> tokenFor (URI service)
    {
        return Single.fromCallable(() -> {
            var name = "HTTP@" + service.getHost();
            var tok = fplus.gssClient()
                .createContextHB(name)
                .flatMap(ctx -> {
                    try {
                        return Optional.of(ctx.initSecContext(new byte[0], 0, 0));
                    }
                    catch (GSSException e) {
                        log.error("GSS error for client: {}", e.toString());
                        return Optional.<byte[]>empty();
                    }
                })
                .map(t -> Base64.getEncoder().encodeToString(t))
                .orElseThrow(() -> new Exception("Can't get GSS token"));

            var req = makeRequest("POST", service, "token",
                "Negotiate " + tok);

            return fetch(req)
                .map(o -> (JSONObject)o)
                .map(o -> o.getString("token"))
                .orElseThrow(() -> 
                    new Exception("GSSAPI fetch failed for token"));
        })
        .subscribeOn(fplus.getScheduler());
    }

    private Optional<Object> fetch (Request req)
    {
        try {
            FPThreadUtil.logId("fetch called");
            Response rsp = req.execute(http_client);
            String body = rsp.returnContent().asString();
            //log.info("Fetch: rsp: {}", body);
            
            if (body == null) return Optional.empty();
            return Optional.of(
                new JSONTokener(body).nextValue());
        }
        catch (HttpResponseException e) {
            int st = e.getStatusCode();
            if (st != 404)
                log.error("Fetch HTTP error: {}", st);
            return Optional.empty();
        }
        catch (Exception e) {
            log.info("Fetch error: {}", e);
            return Optional.empty();
        }
    }
}
