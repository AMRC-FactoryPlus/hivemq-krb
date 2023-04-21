/* Factory+ HiveMQ auth plugin.
 * F+ Auth client.
 * Copyright 2023 AMRC.
 */

/* This implementation is incomplete and does not map all endpoints.
 * Unmapped endpoints can be accessed through the generic http()
 * interface of FPServiceClient. */

package uk.co.amrc.factoryplus;

import java.net.*;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.*;

import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.core.*;

import uk.co.amrc.factoryplus.http.*;

public class FPAuth {
    private static final Logger log = LoggerFactory.getLogger(FPAuth.class);
    private static final UUID SERVICE = FPUuid.Service.Authentication;

    private FPServiceClient fplus;

    public FPAuth (FPServiceClient fplus)
    {
        this.fplus = fplus;
    }

    public Single<Stream<Map>> getACL (String princ, UUID perms)
    {
        return fplus.http().request(SERVICE, "GET")
            .withURIBuilder(b -> b
                .appendPath("authz/acl")
                .setParameter("principal", princ)
                .setParameter("permission", perms.toString()))
            .fetch()
            .cast(JSONArray.class)
            .doOnSuccess(acl -> log.info("F+ ACL [{}]: {}", princ, acl))
            .map(acl -> acl.toList().stream()
                .filter(o -> o instanceof Map)
                .map(o -> (Map)o));
    }
}
