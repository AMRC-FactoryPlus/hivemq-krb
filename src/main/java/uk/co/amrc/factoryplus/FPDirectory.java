/* Factory+ Java client library.
 * Directory service.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.*;

import io.reactivex.rxjava3.core.*;

import uk.co.amrc.factoryplus.http.*;

public class FPDirectory {
    private static final Logger log = LoggerFactory.getLogger(FPDirectory.class);
    private static final UUID SERVICE = FPUuid.Service.Directory;

    private FPServiceClient fplus;

    public FPDirectory (FPServiceClient fplus)
    {
        this.fplus = fplus;
    }

    public Single<Set<URI>> getServiceURLs (UUID service)
    {
        log.info("Looking up {} via the Directory", service);
        return fplus.http().request(SERVICE, "GET")
            .withURIBuilder(b -> b
                .appendPath("v1/service")
                .appendPath(service.toString())
            )
            .fetch()
            .map(res -> res.ifOk()
                .flatMap(r -> r.getBody())
                .orElseGet(() -> {
                    log.error("Can't find {} via the Directory: {}",
                        service, res.getCode());
                    return new JSONArray();
                }))
            .cast(JSONArray.class)
            .flatMapObservable(Observable::fromIterable)
            //.doOnNext(o -> log.info("Service URL: {}", o))
            .cast(JSONObject.class)
            .map(o -> o.getString("url"))
            .map(URI::new)
            .collect(Collectors.toUnmodifiableSet());
    }
}
