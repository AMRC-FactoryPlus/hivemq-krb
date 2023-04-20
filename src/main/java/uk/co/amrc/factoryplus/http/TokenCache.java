/* Factory+ Java client library.
 * HTTP token cache.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus.http;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.json.JSONObject;

import io.reactivex.rxjava3.core.Single;

class TokenCache
{
    private Function<URI, Single<String>> source;
    private ConcurrentHashMap<URI, Single<String>> tokens;

    public TokenCache (Function<URI, Single<String>> tokenSource)
    {
        source = tokenSource;
        tokens = new ConcurrentHashMap<URI, Single<String>>();
    }

    public Single<String> get (URI service)
    {
        return tokens.computeIfAbsent(service,
            srv -> source.apply(service).cache());
    }

    public void remove (URI service, String token)
    {
        tokens.remove(service, token);
    }
}
