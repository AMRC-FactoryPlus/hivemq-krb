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

public abstract class FPGssPrincipal {
    private static final Logger log = LoggerFactory.getLogger(FPGssServer.class);

    FPGssProvider provider;
    Subject subject;

    public FPGssPrincipal (FPGssProvider provider, Subject subject)
    {
        this.provider = provider;
        this.subject = subject;
    }

    public abstract String getPrincipal ();

    public <T> Optional<T> withSubject (String msg, 
        PrivilegedExceptionAction<T> action)
    {
        try {
            return Optional.of(Subject.doAs(subject, action));
        }
        catch (PrivilegedActionException e) {
            log.error(msg, e.toString());
            return Optional.<T>empty();
        }
    }
}
