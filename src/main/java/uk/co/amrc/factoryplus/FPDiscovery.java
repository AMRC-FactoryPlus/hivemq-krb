/* Factory+ Java client library.
 * Service discovery.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.*;

import io.reactivex.rxjava3.core.*;

import uk.co.amrc.factoryplus.http.*;

public class FPDiscovery {
    private static final Logger log = LoggerFactory.getLogger(FPDiscovery.class);
    private static final UUID SERVICE = FPUuid.Service.Directory;

    private FPServiceClient fplus;
    private RequestCache<UUID, Set<URI>> cache;

    public FPDiscovery (FPServiceClient fplus)
    {
        this.fplus = fplus;
        this.cache = new RequestCache<UUID, Set<URI>>(this::_lookup);

        var url = fplus.getUriConf("directory_url");
        log.info("Using Directory {}", url);
        setServiceURL(SERVICE, url);
    }

    public void setServiceURL (UUID service, URI url)
    {
        cache.put(service, Set.of(url));
    }

    public Single<Set<URI>> lookup (UUID service)
    {
        return cache.get(service);
    }

    private Single<Set<URI>> _lookup (UUID service)
    {
        log.info("Looking up {} via the Directory", service);
        return fplus.http().request(SERVICE, "GET")
            .withURIBuilder(b -> b
                .appendPath("v1/service")
                .appendPath(service.toString())
            )
            .fetch()
            .doOnSuccess(o -> log.info("Service resp: {}", o))
            .cast(JSONArray.class)
            .flatMapObservable(Observable::fromIterable)
            .doOnNext(o -> log.info("Service URL: {}", o))
            .cast(JSONObject.class)
            .map(o -> o.getString("url"))
            .map(URI::new)
            .collect(Collectors.toUnmodifiableSet());
    }

    public Single<URI> get (UUID service)
    {
        return lookup(service)
            .flatMap(urls -> urls.isEmpty()
                ? Single.<Set<URI>>error(new Exception("Cannot find service URL"))
                : Single.just(urls))
            /* Just take the first (only) for now. */
            .map(urls -> fixPath(urls.iterator().next()))
            .doOnSuccess(base -> log.info("Resolved {} to {}", service, base));
    }

    public void remove (UUID service, URI bad)
    {
        /* Ignore for now. */
        //cache.remove(service, bad);
    }

    /* Java's URI class doesn't resolve relative URIs properly unless
     * there is an explicit path component. */
    private URI fixPath (URI uri)
    {
        return uri.getPath().length() == 0 ? uri.resolve("/") : uri;
    }

}
