/* Factory+ HiveMQ auth plugin.
 * Kerberos authentication.
 * Copyright 2022 AMRC
 */

package uk.co.amrc.factoryplus.hivemq_auth_krb;

import java.lang.Runnable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.ietf.jgss.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.*;

import com.hivemq.extension.sdk.api.async.*;
import com.hivemq.extension.sdk.api.client.parameter.Listener;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.auth.EnhancedAuthenticator;
import com.hivemq.extension.sdk.api.auth.parameter.*;
import com.hivemq.extension.sdk.api.packets.auth.*;
import com.hivemq.extension.sdk.api.packets.connect.*;
import com.hivemq.extension.sdk.api.services.Services;

public class FPKrbAuth implements EnhancedAuthenticator {

    private static final @NotNull Logger log = LoggerFactory.getLogger(FPKrbAuth.class);

    private FPKrbAuthProvider provider;

    static class GssResult {
        public final byte[] token;
        public final String client;

        private GssResult (byte[] token, String client)
        {
            this.token = token;
            this.client = client;
        }

        public static GssResult accept (GSSContext ctx, byte[] in_buf)
            throws Exception
        {
            /* It would be helpful to log the client and server
             * identities if this call fails, so we can see who was
             * trying to connect and what endpoint they were trying to
             * connect to. But get{Src,Targ}Name can't be called until
             * the context is established, so we can't. Grrr. */
            var token = ctx.acceptSecContext(in_buf, 0, in_buf.length);
            if (!ctx.isEstablished())
                throw new Exception("GSS login took more than one step!");

            var client = ctx.getSrcName().toString();

            return new GssResult(token, client);
        }
    }

    static class AuthResult {
        public GssResult gss;
        public List<TopicPermission> acl;

        public AuthResult (GssResult gss, List<TopicPermission> acl)
        {
            this.gss = gss;
            this.acl = acl;
        }

        public void applyACL (EnhancedAuthOutput output)
        {
            ModifiableDefaultPermissions perms = output.getDefaultPermissions();
            perms.addAll(acl);
            perms.setDefaultBehaviour(DefaultAuthorizationBehaviour.DENY);
        }

        public List<String> showACL ()
        {
            return acl.stream()
                .map(ace -> String.format("%s(%s)", 
                    ace.getActivity(), ace.getTopicFilter()))
                .collect(Collectors.toList());
        }
    }

    public FPKrbAuth (FPKrbAuthProvider prov)
    {
        provider = prov;
    }

    @Override
    public void onConnect (EnhancedAuthConnectInput input, EnhancedAuthOutput output)
    {
        final ConnectPacket conn = input.getConnectPacket();

        String mech = conn.getAuthenticationMethod().orElse(null);

        log.info("CONNECT mech {}", mech);

        if (mech == null) {
            auth_none(conn, output);
            return;
        }
        if (mech.equals("GSSAPI")) {
            auth_gssapi(conn, output);
            return;
        }

        log.info("Unknown auth mech {}", mech);
        output.failAuthentication();
    }

    @Override
    public void onAuth (EnhancedAuthInput input, EnhancedAuthOutput output)
    {
        log.error("Unexpected multi-step auth attempt");
        output.failAuthentication();
        return;
    }

    private void auth_gssapi (ConnectPacket conn, EnhancedAuthOutput output)
    {
        ByteBuffer in_bb = conn.getAuthenticationData().orElse(null);
        if (in_bb == null) {
            log.error("No GSS step data provided");
            output.failAuthentication();
            return;
        }

        byte[] in_buf = new byte[in_bb.limit()];
        in_bb.get(in_buf);

        final Async<EnhancedAuthOutput> asyncOutput = output.async(
            Duration.ofSeconds(10), TimeoutFallback.FAILURE);

        verify_gssapi(in_buf)
            .subscribeOn(provider.getScheduler())
            .doAfterTerminate(() -> asyncOutput.resume())
            .subscribe(
                rv -> {
                    rv.applyACL(output);
                    output.authenticateSuccessfully(rv.gss.token);
                },
                e -> {
                    log.error("GSSAPI auth failed", e);
                    output.failAuthentication();
                });
    }

    private void auth_none (ConnectPacket conn, EnhancedAuthOutput output)
    {
        String user = conn.getUserName().orElse(null);
        ByteBuffer passwd = conn.getPassword().orElse(null);

        if (user == null || passwd == null) {
            log.error("Null username/password, failing auth");
            output.failAuthentication();
            return;
        }

        /* XXX should passwords be UTF-8? */
        CharBuffer passwd_c = StandardCharsets.UTF_8.decode(passwd);
        char[] passwd_buf = new char[passwd_c.limit()];
        passwd_c.get(passwd_buf);

        final Async<EnhancedAuthOutput> asyncOutput = output.async(
            Duration.ofSeconds(10), TimeoutFallback.FAILURE);

        provider.proxyForClient(user, passwd_buf)
            .subscribeOn(provider.getScheduler())
            .flatMap(buf -> verify_gssapi(buf))
            .doFinally(() -> asyncOutput.resume())
            .subscribe(
                rv -> {
                    rv.applyACL(output);
                    output.authenticateSuccessfully();
                },
                e ->  {
                    log.error("Password authentication failed for {}: {}",
                        user, e.toString());
                    output.failAuthentication();
                });
    }

    private Single<AuthResult> verify_gssapi (byte[] in_buf)
    {
            /* blocking */
        return Single.using(
                () -> provider.createServerContext(),
                ctx -> Single.just(GssResult.accept(ctx, in_buf)),
                ctx -> ctx.dispose())
            .doOnSuccess(gsr -> log.info("Authenticated client {}", gsr.client))
            .flatMap(gsr -> provider.getACLforPrincipal(gsr.client)
                .map(acl -> new AuthResult(gsr, acl)))
            .doOnSuccess(rv -> log.info("MQTT ACL [{}]: {}", 
                rv.gss.client, rv.showACL()));
    }
}
