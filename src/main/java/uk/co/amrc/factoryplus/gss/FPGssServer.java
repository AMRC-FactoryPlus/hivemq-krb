/* Factory+ Java client library.
 * GSS principal helper.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus.gss;

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

public class FPGssServer extends FPGssPrincipal {
    private static final Logger log = LoggerFactory.getLogger(FPGssServer.class);

    private GSSCredential creds;

    public FPGssServer (FPGssProvider provider, String principal, Subject subject)
    {
        super(provider, principal, subject);
    }

    public Optional<FPGssServer> login ()
    {
        return withSubject("getting creds from keytab", () -> {
            creds = provider.getGSSManager()
                .createCredential(GSSCredential.ACCEPT_ONLY);

            log.info("Got GSS creds for server:");
            for (Oid mech : creds.getMechs()) {
                log.info("  Oid {}, name {}", 
                    mech, creds.getName(mech));
            }

            return this;
        });
    }

    public Optional<GSSContext> createContext ()
    {
        return withSubject("creating server context",
                () -> provider.getGSSManager().createContext(creds));
    }
}