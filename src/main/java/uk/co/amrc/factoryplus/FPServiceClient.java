/* Factory+ HiveMQ auth plugin.
 * F+ service client.
 * Copyright 2022 AMRC.
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

public class FPServiceClient {
    private static final Logger log = LoggerFactory.getLogger(FPServiceClient.class);

    private Map<String, String> config;

    private FPGssProvider _gss;
    private FPGssServer _gss_server;
    private FPHttpClient _http;

    private URI authn_service;
    private URI configdb_service;

    public FPServiceClient () { 
        this(Map.<String,String>of());
    }

    public FPServiceClient (Map config)
    {
        this.config = config;

        authn_service = getUriConf("authn_url");
        configdb_service = getUriConf("configdb_url");
    }

    public String getConf (String key)
    {
        if (config.containsKey(key))
            return config.get(key);

        String env = key.toUpperCase(Locale.ROOT);
        String val = System.getenv(env);
        if (val == null || val == "")
            throw new ServiceConfigurationError(
                String.format("Environment variable %s must be set!", env));

        return val;
    }

    public URI getUriConf (String env)
    {
        String uri = getConf(env);
        try {
            return new URI(uri);
        }
        catch (URISyntaxException e) {
            throw new ServiceConfigurationError(
                String.format("Bad URI for %s: %s", env, uri));
        }
    }

    public FPGssProvider gss ()
    {
        if (_gss == null)
            _gss = new FPGssProvider();
        return _gss;
    }

    public FPGssServer gssServer ()
    {
        if (_gss_server == null) {
            String princ = getConf("server_principal");
            String keytab = getConf("server_keytab");

            _gss_server = gss().server(princ, keytab)
                .flatMap(s -> s.login())
                .orElseThrow(() -> new ServiceConfigurationError(
                    "Cannot get server GSS creds"));
        }

        return _gss_server;
    }

    public FPHttpClient http ()
    {
        if (_http == null)
            _http = new FPHttpClient(this);
        return _http;
    }

    public Stream<String> configdb_list_objects (String appid)
    {
        URIBuilder path = new URIBuilder(configdb_service)
            .appendPath("/v1/app")
            .appendPath(appid)
            .appendPath("object");

        return http().fetch("GET", path, null)
            .stream()
            .filter(o -> o instanceof JSONArray)
            .map(o -> (JSONArray)o)
            .flatMap(ary -> ary.toList().stream())
            .filter(o -> o instanceof String)
            .map(o -> (String)o);
    }

    public Optional<JSONObject> configdb_fetch_object (String appid, String objid)
    {
        URIBuilder path = new URIBuilder(configdb_service)
            .appendPath("/v1/app")
            .appendPath(appid)
            .appendPath("object")
            .appendPath(objid);

        return http().fetch("GET", path, null)
            .map(o -> o instanceof JSONObject ? (JSONObject)o : null);
    }

    public Stream<Map> authn_acl (String princ, String perms)
    {
        URIBuilder acl_url = new URIBuilder(authn_service)
            .appendPath("/authz/acl")
            .setParameter("principal", princ)
            .setParameter("permission", perms);

        Optional<JSONArray> acl = http().fetch("GET", acl_url, null)
            .map(o -> o instanceof JSONArray ? (JSONArray)o : null);
        log.info("F+ ACL [{}]: {}", princ, acl.orElse(null));

        return acl.stream()
            .flatMap(ary -> ary.toList().stream())
            .filter(o -> o instanceof Map)
            .map(o -> (Map)o);
    }
}

