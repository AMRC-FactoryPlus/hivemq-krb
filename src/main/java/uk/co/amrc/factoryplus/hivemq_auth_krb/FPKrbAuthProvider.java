/* Factory+ HiveMQ auth plugin.
 * Authentication provider.
 * Copyright 2022 AMRC.
 */

package uk.co.amrc.factoryplus.hivemq_auth_krb;

import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.concurrent.Callable;
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
    private static final String PERMGRP_UUID = "a637134a-d06b-41e7-ad86-4bf62fde914a";
    private static final String TEMPLATE_UUID = "1266ddf1-156c-4266-9808-d6949418b185";
    private static final String ADDR_UUID = "8e32801b-f35a-4cbf-a5c3-2af64d3debd7";

    private FPServiceClient fplus;

    public FPKrbAuthProvider ()
    {
        fplus = new FPServiceClient();
        /* HiveMQ recommend using their thread pool. However it looks
         * like this only has one thread, which means all our fetch
         * calls get serialised. So for now stick to using our own. */
        //fplus.setExecutor(Services.extensionExecutorService());
    }

    @Override
    public EnhancedAuthenticator getEnhancedAuthenticator (final AuthenticatorProviderInput input)
    {
        return new FPKrbAuth(this);
    }

    public GSSContext createServerContext ()
    {
        return fplus.gssServer()
            .createContext()
            .orElseThrow(() -> new ServiceConfigurationError(
                "Cannot create server GSS context"));
    }

    public Optional<GSSContext> createProxyContext (String user, char[] passwd)
    {
        String srv = fplus.gssServer().getPrincipal();
        return fplus.gss()
            .clientWithPassword(user, passwd)
            .flatMap(cli -> cli.login())
            .flatMap(cli -> cli.createContext(srv));
    }

    public Single<Stream<TopicPermission>> getACLforPrincipal (String principal)
    {
        return fplus.authn_acl(principal, PERMGRP_UUID)
            .map(acl -> acl
                .flatMap(ace -> {
                    String perm = (String)ace.get("permission");
                    String targid = (String)ace.get("target");

                    JSONObject template = fplus
                        .configdb_fetch_object(TEMPLATE_UUID, perm)
                        .orElseGet(() -> new JSONObject());

                    Callable<JSONObject> target = () -> fplus
                        .configdb_fetch_object(ADDR_UUID, targid)
                        .orElseGet(() -> new JSONObject());

                    return template.toMap()
                        .entrySet().stream()
                        .map(e -> new MqttAce(e.getKey(), e.getValue(), target))
                        .filter(m_ace -> m_ace.getActivity() != null)
                        .map(m_ace -> m_ace.toTopicPermission());
                }));
    }
}
