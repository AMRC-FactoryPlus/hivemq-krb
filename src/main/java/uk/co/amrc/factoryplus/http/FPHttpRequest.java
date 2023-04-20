/* Factory+ Java client library.
 * HTTP request.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus.http;

import java.util.UUID;
import java.util.function.UnaryOperator;

import org.apache.hc.core5.net.URIBuilder;
import org.json.JSONObject;

import io.reactivex.rxjava3.core.Single;

public class FPHttpRequest {
    /* These are package-scoped; FPHttpClient wants 'friend' access. */
    FPHttpClient client;
    UUID service;
    String method;
    String path;
    JSONObject body;

    public FPHttpRequest (FPHttpClient client, UUID service, String method)
    {
        this.client = client;
        this.service = service;
        this.method = method;
    }

    public FPHttpRequest withPath (String path)
    {
        this.path = path;
        return this;
    }

    public FPHttpRequest withURIBuilder (UnaryOperator<URIBuilder> build)
    {
        var dot = new URIBuilder()
            .setPathSegmentsRootless(".");
        this.path = build.apply(dot).toString();
        return this;
    }

    public FPHttpRequest withBody (JSONObject body)
    {
        this.body = body;
        return this;
    }

    public Single<Object> fetch ()
    {
        return client.execute(this);
    }
}
