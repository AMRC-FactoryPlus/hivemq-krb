/* Factory+ HiveMQ auth plugin.
 * Authentication provider.
 * Copyright 2022 AMRC.
 */

package uk.co.amrc.factoryplus.hivemq_auth_krb;

import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ietf.jgss.*;
import org.json.*;
import org.apache.hc.core5.net.URIBuilder;

import io.reactivex.rxjava3.core.*;

import uk.co.amrc.factoryplus.*;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.auth.EnhancedAuthenticator;
import com.hivemq.extension.sdk.api.auth.parameter.AuthenticatorProviderInput;
import com.hivemq.extension.sdk.api.services.auth.provider.EnhancedAuthenticatorProvider;
import com.hivemq.extension.sdk.api.auth.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;

public class FPKrbAuthProvider implements EnhancedAuthenticatorProvider
{
    private static final Logger log = LoggerFactory.getLogger(FPKrbAuth.class);

    private static final UUID PERMGRP_UUID = UUID.fromString(
        "a637134a-d06b-41e7-ad86-4bf62fde914a");
    private static final UUID TEMPLATE_UUID = UUID.fromString(
        "1266ddf1-156c-4266-9808-d6949418b185");
    private static final UUID ADDR_UUID = UUID.fromString(
        "8e32801b-f35a-4cbf-a5c3-2af64d3debd7");

    private FPServiceClient fplus;

    public FPKrbAuthProvider ()
    {
        fplus = new FPServiceClient();
    }

    public FPKrbAuthProvider start ()
    {
        fplus.http().start();

         var url = fplus.getUriConf("mqtt_url");

        fplus.directory()
            .registerServiceURL(FPUuid.Service.MQTT, url)
            .retryWhen(errs -> errs
                .doOnNext(e -> {
                    log.error("Service registration failed: {}", e.toString());
                    log.info("Retrying registration in 5 seconds.");
                })
                .delay(5, TimeUnit.SECONDS))
            .timeout(10, TimeUnit.MINUTES)
            .subscribe(() -> log.info("Registered service successfully"),
                e -> log.error("Failed to register service: {}", 
                    e.toString()));

        return this;
    }

    @Override
    public EnhancedAuthenticator getEnhancedAuthenticator (final AuthenticatorProviderInput input)
    {
        return new FPKrbAuth(this);
    }

    public Scheduler getScheduler () { return fplus.getScheduler(); }

    public GSSContext createServerContext ()
    {
        return fplus.gssServer()
            .createContext()
            .orElseThrow(() -> new ServiceConfigurationError(
                "Cannot create server GSS context"));
    }

    public Single<byte[]> proxyForClient (String user, char[] passwd)
    {
        /* This whole call is synchronous and blocking. I don't think
         * there's much point trying to avoid this: the blocking calls
         * are all GSSAPI, which is always synchronous. */
        /* I'm not convinced the Optional<> returns are helping any
         * more. Probably they should be changed to Single<>, or just
         * left throwing. */
        return Single.fromCallable(() -> {
            var cli = fplus.gss().clientWithPassword(user, passwd)
                .orElseThrow(() -> new Exception(
                    "Password login failed for " + user));
            try {
                cli.login();

                String srv = fplus.gssServer().getPrincipal();
                var ctx = cli.createContext(srv)
                    .orElseThrow(() -> new Exception(
                        "Cannot create proxy GSS context"));

                try {
                    return ctx.initSecContext(new byte[0], 0, 0);
                }
                finally {
                    ctx.dispose();
                }
            }
            finally {
                cli.dispose();
            }
        });
    }

    public Single<List<TopicPermission>> getACLforPrincipal (String principal)
    {
        /* This will not send a request until someone subscribes. */
        var princUUID = fplus.auth()
            .getPrincipalByKerberos(principal);

        return fplus.auth().getACL(principal, PERMGRP_UUID)
            .flatMapObservable(Observable::fromStream)
            .flatMap(ace -> fplus.configdb()
                .getConfig(TEMPLATE_UUID, ace.permission)
                .flattenStreamAsObservable(tmpl ->
                    tmpl.toMap().entrySet().stream())
                .map(e -> MqttAce.fromEntry(e, ace.target)))
            .collect(Collectors.toList())
            .flatMapObservable(acl -> {
                var wantPrinc = acl.stream()
                    .anyMatch(ace -> ace.uses(MqttAce.WANT_PRINC));

                var uuids = Observable.fromStream(acl.stream())
                    .filter(ace -> ace.uses(MqttAce.WANT_TARG))
                    .map(ace -> ace.getTarget())
                    .mergeWith(wantPrinc
                        ? fplus.auth()
                            .getPrincipalByKerberos(principal)
                        : Maybe.<UUID>empty())
                    .distinct();

            .flatMapStream(ace -> {
                final var entries = ace.template.entrySet();
                final int want = entries.stream()
                    .map(
                
                Single<JSONObject> target = fplus.configdb()
                    .getConfig(ADDR_UUID, ace.target);
                return ace.template.entrySet().stream()
                    .map(e -> MqttAce.expandEntry(e, target));
            })
            .flatMap(Observable::fromMaybe)
            .map(m_ace -> m_ace.toTopicPermission())
            .collect(Collectors.toList());
    }
}
