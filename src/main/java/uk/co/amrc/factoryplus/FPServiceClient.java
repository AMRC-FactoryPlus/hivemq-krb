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
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hc.core5.net.URIBuilder;
import org.json.*;

import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.core.*;

import uk.co.amrc.factoryplus.gss.*;

public class FPServiceClient {
    private static final Logger log = LoggerFactory.getLogger(FPServiceClient.class);

    private Map<String, String> config;
    private Executor _executor;
    private Scheduler _scheduler;

    private FPGssProvider _gss;
    private FPGssServer _gss_server;
    private FPHttpClient _http;
    private FPDiscovery _discovery;

    private URI configdb_service;

    public FPServiceClient () { 
        this(Map.<String,String>of());
    }

    public FPServiceClient (Map config)
    {
        this.config = config;
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

    synchronized public void setExecutor (Executor exec)
    {
        if (_scheduler != null)
            throw new IllegalStateException(
                "Can't set executor: scheduler has already been created");

        _executor = exec;
    }

    synchronized public Scheduler getScheduler ()
    {
        if (_scheduler == null) {
            var exec = _executor != null ? _executor
                : Executors.newScheduledThreadPool(4);
            _scheduler = Schedulers.from(exec, true, true);
        }
        return _scheduler;
    }

    synchronized public FPGssProvider gss ()
    {
        if (_gss == null)
            _gss = new FPGssProvider();
        return _gss;
    }

    synchronized public FPGssServer gssServer ()
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

    synchronized public FPHttpClient http ()
    {
        if (_http == null)
            _http = new FPHttpClient(this);
        return _http;
    }

    synchronized public FPDiscovery discovery ()
    {
        if (_discovery == null)
            _discovery = new FPDiscovery(this);
        return _discovery;
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
        Set<URI> urls = discovery().lookup(FPUuid.Service.Authentication);
        if (urls.isEmpty())
            return Stream.<Map>empty();
        /* Just take the first (only) for now. */
        URI authn_service = urls.iterator().next();
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

