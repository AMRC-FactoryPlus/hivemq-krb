/* Factory+ Java client library.
 * GSS helper class.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus;

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

public class FPGss {
    private static final Logger log = LoggerFactory.getLogger(FPGss.class);

    GSSManager gss_manager;
    Oid krb5Mech, krb5PrincipalNT;

    public FPGss ()
    {
        try {
            krb5Mech = new Oid("1.2.840.113554.1.2.2");
            krb5PrincipalNT = new Oid("1.2.840.113554.1.2.2.1");

            gss_manager = GSSManager.getInstance();
        }
        catch (GSSException e) {
            throw new ServiceConfigurationError("Cannot initialise GSSAPI", e);
        }
    }

    public GSSManager getGSSManager () { return gss_manager; }

    public Optional<Subject> buildSubject (
        String type, CallbackHandler cb, Configuration config
    ) {
        Subject subj = new Subject();

        try {
            LoginContext ctx = new LoginContext(type, subj, cb, config);
            ctx.login();
            return Optional.of(subj);
        }
        catch (LoginException e) {
            log.error("Krb5 init failed: {}", e.toString());
            return Optional.<Subject>empty();
        }
    }

    public Optional<Subject> buildServerSubject (String keytab, String principal)
    {
        Configuration config = new KrbConfiguration(principal, keytab);
        CallbackHandler cb = new NullCallbackHandler();
        return buildSubject("server", cb, config);
    }

    public Optional<Subject> buildClientSubjectWithCcache (String principal)
    {
        Configuration config = new KrbConfiguration(principal);
        CallbackHandler cb = new NullCallbackHandler();
        return buildSubject("client-keytab", cb, config);
    }

    public Optional<Subject> buildClientSubjectWithPassword (
        char[] password, String principal
    ) {
        Configuration config = new KrbConfiguration(principal);
        CallbackHandler cb = new PasswordCallbackHandler(principal, password);
        return buildSubject("client-password", cb, config);
    }

    public Optional<FPGssServer> server (String principal, String keytab)
    {
        return buildServerSubject(keytab, "*")
            .map(subj -> new FPGssServer(this, principal, subj));
    }

    public Optional<FPGssClient> clientWithCcache (String username)
    {
        return buildClientSubjectWithCcache(username)
            .map(subj -> new FPGssClient(this, username, subj));
    }

    public Optional<FPGssClient> clientWithPassword (String username, char[] password)
    {
        return buildClientSubjectWithPassword(password, username)
            .map(subj -> new FPGssClient(this, username, subj));
    }
}
