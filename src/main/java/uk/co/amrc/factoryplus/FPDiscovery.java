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
    private ConcurrentHashMap<UUID, Set<URI>> cache;
    private ConcurrentHashMap<UUID, Single<Set<URI>>> inFlight;

    public FPDiscovery (FPServiceClient fplus)
    {
        this.fplus = fplus;
        this.cache = new ConcurrentHashMap<UUID, Set<URI>>();
        this.inFlight = new ConcurrentHashMap<UUID, Single<Set<URI>>>();

        setServiceURL(SERVICE, fplus.getUriConf("directory_url"));
    }

    public void setServiceURL (UUID service, URI url)
    {
        /* XXX If we have an in-flight request we need to cancel it, and
         * arrange for the lookup() call to return this URL instead.
         * Otherwise it will overwrite the URL we set here. */
        cache.put(service, Set.of(url));
    }

    public Single<Set<URI>> lookup (UUID service)
    {
        var rv = cache.get(service);
        if (rv != null)
            return Single.just(rv);

        return inFlight.computeIfAbsent(service, srv -> {
            var promise = _lookup(srv);
            log.info("In-flight: add {} {}", srv, promise);
            promise
                .doAfterTerminate(() -> {
                    log.info("In-flight: remove {} {}", srv, promise);
                    inFlight.remove(srv, promise);
                })
                .subscribe(urls -> cache.put(srv, urls));
            return promise;
        });
    }

    private Single<Set<URI>> _lookup (UUID service)
    {
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
            .collect(Collectors.toUnmodifiableSet())
            .cache();
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
        cache.remove(service, bad);
    }

    /* Java's URI class doesn't resolve relative URIs properly unless
     * there is an explicit path component. */
    private URI fixPath (URI uri)
    {
        return uri.getPath().length() == 0 ? uri.resolve("/") : uri;
    }

}
