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

import javax.security.auth.Subject;
import org.ietf.jgss.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        }
        else {
            byte[] in_buf = new byte[in_bb.limit()];
            in_bb.get(in_buf);

            final Async<EnhancedAuthOutput> asyncOutput = output.async(
                Duration.ofSeconds(10), TimeoutFallback.FAILURE);

            Services.extensionExecutorService().submit(() -> {
                verify_gssapi(in_buf, output, true);
                asyncOutput.resume();
            });
        }
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

        Services.extensionExecutorService().submit(() -> {
            /* We need to get and verify a service ticket, to protect
             * against a spoofed KDC. The only striaghtforward way to do
             * this is just to do the whole GSSAPI dance on the client's
             * behalf. */
            byte[] gss_buf = get_client_gss_proxy(user, passwd_buf);
            if (gss_buf == null) {
                log.error("Password authentication failed for {}", user.toString());
                output.failAuthentication();
            }
            else {
                verify_gssapi(gss_buf, output, false);
            }
            asyncOutput.resume();
        });
    }

    private byte[] get_client_gss_proxy (String user, char[] passwd_buf)
    {
        Subject client = provider.getSubjectWithPassword(user, passwd_buf);
        if (client == null) {
            return null;
        }

        return Subject.doAs(client, (PrivilegedAction<byte[]>)() -> {
            try {
                GSSContext ctx = provider.createProxyContext();
                return ctx.initSecContext(new byte[0], 0, 0);
            }
            catch (GSSException e) {
                log.error("Client GSS exception: {}", e.toString());
                return null;
            }
        });
    }

    private void verify_gssapi (byte[] in_buf, EnhancedAuthOutput output, boolean send_auth)
    {
        GSSContext ctx = provider.createServerContext();

        try {
            /* It would be helpful to log the client and server
             * identities if this call fails, so we can see who was
             * trying to connect and what endpoint they were trying to
             * connect to. But get{Src,Targ}Name can't be called until
             * the context is established, so we can't. Grrr. */
            byte[] out_buf = ctx.acceptSecContext(in_buf, 0, in_buf.length);

            if (ctx.isEstablished()) {
                String client_name = ctx.getSrcName().toString();
                log.info("Authenticated client {}", client_name);
                setupACLs(output, client_name);
                if (send_auth) 
                    output.authenticateSuccessfully(out_buf);
                else
                    output.authenticateSuccessfully();
            }
            else {
                /* We could handle this case, but I don't think with the
                 * Kerberos mech there is ever any need. */
                log.error("GSS login took more than one step!");
                output.failAuthentication();
            }
        }
        catch (GSSException e) {
            log.error("GSS login failed: {}", e);
            output.failAuthentication();
        }
    }

    private void setupACLs (EnhancedAuthOutput output, String principal)
    {
        List<TopicPermission> acl = provider.getACLforPrincipal(principal);

        log.info("MQTT ACL [{}]: {}", principal, acl.stream()
            .map(ace -> String.format("%s(%s)", ace.getActivity(), ace.getTopicFilter()))
            .collect(Collectors.toList()));

        ModifiableDefaultPermissions perms = output.getDefaultPermissions();
        perms.addAll(acl);
        perms.setDefaultBehaviour(DefaultAuthorizationBehaviour.DENY);
    }
}
