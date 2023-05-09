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

import io.reactivex.rxjava3.disposables.Disposable;

/** A GSS principal (client or server).
 */
public abstract class FPGssPrincipal
    implements Disposable
{
    private static final Logger log = LoggerFactory.getLogger(FPGssServer.class);

    protected final FPGssProvider provider;

    private Subject subject;
    private GSSCredential creds;

    /** Internal, construct via {@link FPGssProvider}. */
    public FPGssPrincipal (FPGssProvider provider, Subject subject)
    {
        this.provider = provider;
        this.subject = subject;
    }

    synchronized public boolean isDisposed ()
    {
        return subject == null;
    }

    synchronized public void dispose ()
    {
        if (isDisposed()) return;
        subject = null;
        if (creds != null) {
            try {
                creds.dispose();
            }
            catch (GSSException e) {
                log.error("Error disposing of GSS credentials: {}",
                    e.toString());
            }
        }
    }

    /** Gets our Kerberos principal name.
     *
     * @return Our Kerberos principal name.
     */
    public abstract String getPrincipal ();

    /** Obtain our Kerberos credentials.
     *
     * This operation may block on network calls.
     *
     * @return This, iff successful.
     */
    public abstract <T extends FPGssProvider> Optional<T> login ();

    /** Performs an operation using our Subject.
     *
     * We hold an internal {@link Subject} representing our security
     * context. This calls 
     * {@link Subject#doAs(Subject, PrivilegedExceptionAction)}
     * and returns the result. If the action throws an exception it will
     * be caught and logged and Optional.empty returned.
     *
     * @param msg A description of this action, for logging an error.
     * @param action The action to perform.
     * @return The result of the action, if successful.
     */
    public <T> Optional<T> withSubject (String msg, 
        PrivilegedExceptionAction<T> action)
    {
        throwIfDisposed();
        try {
            return Optional.of(Subject.doAs(subject, action));
        }
        catch (PrivilegedActionException e) {
            log.error(msg, e.toString());
            return Optional.<T>empty();
        }
    }

    protected void throwIfDisposed ()
    {
        if (isDisposed())
            throw new IllegalStateException("Object has been disposed");
    }

    synchronized protected GSSCredential getCreds ()
    {
        throwIfDisposed();
        if (creds == null)
            throw new IllegalStateException("No GSS credentials available");
        return creds;
    }

    synchronized protected void setCreds (GSSCredential creds)
    {
        throwIfDisposed();
        if (creds != null)
            throw new IllegalStateException("Can't overwrite GSS creds");
        this.creds = creds;
    }
}
