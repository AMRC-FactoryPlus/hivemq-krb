/* Factory+ Java client library.
 * GSS principal helper.
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

public class FPGssClient extends FPGssPrincipal {
    private static final Logger log = LoggerFactory.getLogger(FPGssServer.class);

    private GSSCredential creds;

    public FPGssClient (FPGssProvider provider, String principal, Subject subject)
    {
        super(provider, principal, subject);
    }

    public Optional<FPGssClient> login ()
    {
        return withSubject("getting client credentials", () -> {
            creds = provider.getGSSManager()
                .createCredential(GSSCredential.INITIATE_ONLY);

            log.info("Got GSS creds for client:");
            for (Oid mech : creds.getMechs()) {
                log.info("  Oid {}, name {}", 
                    mech, creds.getName(mech));
            }

            return this;
        });
    }

    public Optional<GSSContext> createContext (String server)
    {
        return withSubject("creating client context", () -> {
            GSSManager mgr = provider.getGSSManager();
            GSSName srv_nam = mgr.createName(server, provider.krb5PrincipalNT());
            return mgr.createContext(
                srv_nam, provider.krb5Mech(), creds, GSSContext.DEFAULT_LIFETIME);
        });
    }
}
