/* Factory+ HiveMQ auth plugin.
 * F+ Auth client.
 * Copyright 2023 AMRC.
 */


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

/**
 * Authentication service.
 *
 * This implementation is incomplete and does not map all endpoints.
 * Unmapped endpoints can be accessed through the generic http()
 * interface of FPServiceClient.
 */
public class FPAuth {
    private static final Logger log = LoggerFactory.getLogger(FPAuth.class);
    private static final UUID SERVICE = FPUuid.Service.Authentication;

    private FPServiceClient fplus;

    class ACE {
        public final UUID permission;
        public final UUID target;

        public ACE (UUID p, UUID t)
        {
            permission = p;
            target = t;
        }

        public static ACE fromMap (Map<String, Object> map)
            throws Exception
        {
            String perm = (String)ace.get("permission");
            String target = (String)ace.get("target");
            
            return new ACE(UUID.fromString(perm),
                UUID.fromString(target));
        }
    }

    public FPAuth (FPServiceClient fplus)
    {
        this.fplus = fplus;
    }

    /**
     * Fetches an ACL.
     *
     * Fetches a list of all permissions granted to the given principal,
     * specified as a Kerberos principal name, in the given permission
     * group.
     *
     * The requesting client must have permission to read the given
     * permission group.
     *
     * @param princ The principal to fetch permissions for.
     * @param perms The permission group to fetch.
     * @return A stream of maps represnting the granted permissions.
     */
    public Single<Stream<ACE>> getACL (String princ, UUID perms)
    {
        //FPThreadUtil.logId("fetching acl");
        return fplus.http().request(SERVICE, "GET")
            .withURIBuilder(b -> b
                .appendPath("authz/acl")
                .setParameter("principal", princ)
                .setParameter("permission", perms.toString()))
            .fetch()
            .map(res -> res.ifOk()
                .flatMap(r -> r.getBodyArray())
                .orElseGet(() -> {
                    log.error("Can't fetch ACL: {}", res.getCode());
                    return new JSONArray();
                }))
            .doOnSuccess(acl -> log.info("F+ ACL [{}]: {}", princ, acl))
            .map(acl -> acl.toList().stream()
                .filter(o -> o instanceof Map)
                .map(o -> ACE.fromMap((Map)o)));
    }

    /**
     * Finds a principal UUID from their Kerberos UPN.
     *
     * The requesting client needs Read_Krb permission on the principal.
     *
     * @param kerberos The Kerberos UPN to look up.
     * @return The UUID of the principal.
     */
    public Maybe<UUID> getPrincipalByKerberos (String kerberos)
    {
        return fplus.http().request(SERVICE, "GET")
            .withURIBuilder(b -> b
                .appendPath("authz/principal/find")
                .setParameter("kerberos", kerberos))
            .fetch()
            .flatMapOptional(res -> res.ifOk());
}
