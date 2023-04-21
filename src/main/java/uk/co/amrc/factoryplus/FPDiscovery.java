/* Factory+ Java client library.
 * Service discovery.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.*;

import uk.co.amrc.factoryplus.http.*;

public class FPDiscovery {
    private static final Logger log = LoggerFactory.getLogger(FPDiscovery.class);
    private static final UUID SERVICE = FPUuid.Service.Directory;

    private FPServiceClient fplus;
    private Map<UUID, Set<URI>> cache;

    public FPDiscovery (FPServiceClient fplus)
    {
        this.fplus = fplus;
        this.cache = Map.of(
            FPUuid.Service.Directory,
                Set.of(fplus.getUriConf("directory_url")),
            FPUuid.Service.Authentication,
                Set.of(fplus.getUriConf("authn_url")),
            FPUuid.Service.ConfigDB,
                Set.of(fplus.getUriConf("configdb_url")));
    }

    public Single<Set<URI>> lookup (UUID service)
    {
        return Single.just(cache.getOrDefault(service, Set.<URI>of()));
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
        /* Ignore for now */
    }

    /* Java's URI class doesn't resolve relative URIs properly unless
     * there is an explicit path component. */
    private URI fixPath (URI uri)
    {
        return uri.getPath().length() == 0 ? uri.resolve("/") : uri;
    }

}
