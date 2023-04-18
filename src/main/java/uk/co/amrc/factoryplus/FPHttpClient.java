/* Factory+ Java client library.
 * HTTP client.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus;

import java.net.*;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.stream.Stream;
import java.util.stream.Collectors;

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

import uk.co.amrc.factoryplus.gss.*;

public class FPHttpClient {
    private static final Logger log = LoggerFactory.getLogger(FPHttpClient.class);

    private FPServiceClient fplus;
    private CloseableHttpClient http_client;
    private String service_auth;

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
    }

    public Optional<Object> fetch (String method, URI uri, JSONObject json)
    {
        try {
            //log.info("Fetch: req: {} {}", method, uri.toString());
            Request req = Request.create(method, uri)
                .setHeader("Authorization", service_auth);

            if (json != null) {
                req.bodyString(json.toString(), ContentType.APPLICATION_JSON);
            }

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

    public Optional<Object> fetch (
        String method, URIBuilder builder, JSONObject json)
    {
        try {
            URI uri = builder.build();
            return fetch(method, uri, json);
        }
        catch (URISyntaxException e) {
            log.error("Fetch for bad URI: {}", e.toString());
            return Optional.empty();
        }
    }
}
