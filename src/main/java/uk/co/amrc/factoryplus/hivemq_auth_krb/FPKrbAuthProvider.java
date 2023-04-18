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

    private FPServiceClient service_client;

    private FPGssProvider gss;
    private FPGssServer gss_server;

    public FPKrbAuthProvider ()
    {
        String princ = safe_getenv("SERVER_PRINCIPAL");
        String keytab = safe_getenv("SERVER_KEYTAB");

        gss = new FPGssProvider();
        gss_server = gss.server(princ, keytab)
            .flatMap(s -> s.login())
            .orElseThrow(() -> new ServiceConfigurationError(
                "Cannot get server GSS creds"));

        service_client = new FPServiceClient();
    }

    @Override
    public EnhancedAuthenticator getEnhancedAuthenticator (final AuthenticatorProviderInput input)
    {
        return new FPKrbAuth(this);
    }

    public GSSContext createServerContext ()
    {
        return gss_server.createContext()
            .orElseThrow(() -> new ServiceConfigurationError(
                "Cannot create server GSS context"));
    }

    public Optional<GSSContext> createProxyContext (String user, char[] passwd)
    {
        return gss.clientWithPassword(user, passwd)
            .flatMap(cli -> cli.login())
            .flatMap(cli -> cli.createContext(gss_server.getPrincipal()));
    }

    public List<TopicPermission> getACLforPrincipal (String principal)
    {
        try {
            return service_client.authn_acl(principal, PERMGRP_UUID)
                .flatMap(ace -> {
                    String perm = (String)ace.get("permission");
                    String targid = (String)ace.get("target");

                    JSONObject template = service_client.configdb_fetch_object(TEMPLATE_UUID, perm);
                    Callable<JSONObject> target = 
                        () -> service_client.configdb_fetch_object(ADDR_UUID, targid);

                    return template.toMap()
                        .entrySet().stream()
                        .map(e -> new MqttAce(e.getKey(), e.getValue(), target))
                        .filter(m_ace -> m_ace.getActivity() != null)
                        .map(m_ace -> m_ace.toTopicPermission());
                })
                .collect(Collectors.toList());
        }
        catch (Exception e) {
            log.error("Error resolving ACL for {}: {}", principal, e.toString());
            return List.of();
        }
    }

    private String safe_getenv (String key)
    {
        String val = System.getenv(key);
        if (val == null)
            throw new ServiceConfigurationError(String.format(
                "%s needs to be set in the environment!", key));
        return val;
    }
}
