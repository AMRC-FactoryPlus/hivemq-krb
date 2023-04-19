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

public class FPDiscovery {
    private static final Logger log = LoggerFactory.getLogger(FPHttpClient.class);

    private FPServiceClient fplus;
    private Map<UUID, Set<URI>> cache;

    public FPDiscovery (FPServiceClient fplus)
    {
        this.fplus = fplus;
        this.cache = Map.of(
            FPUuid.Service.Authentication,
                Set.of(fplus.getUriConf("authn_url")),
            FPUuid.Service.ConfigDB,
                Set.of(fplus.getUriConf("configdb_url")));
    }

    public Set<URI> lookup (UUID service)
    {
        return cache.getOrDefault(service, Set.<URI>of());
    }
}
