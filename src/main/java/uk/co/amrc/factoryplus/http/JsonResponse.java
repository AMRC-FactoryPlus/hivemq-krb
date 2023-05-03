/* Factory+ Java client library.
 * JSON HTTP response.
 * Copyright 2023 AMRC.
 */

package uk.co.amrc.factoryplus.http;

import java.util.Optional;

import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.http.ProtocolException;

import org.json.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonResponse
{
    private static final Logger log = LoggerFactory.getLogger(FPHttpClient.class);

    private SimpleHttpResponse response;
    private Optional<Object> body;

    public JsonResponse (SimpleHttpResponse res)
    {
        response = res;
        log.info("Parsing JSON response: {}", res.getCode());

        body = Optional.ofNullable(res.getBodyText())
            .filter(s -> !s.isEmpty())
            .map(json -> new JSONTokener(json))
            .flatMap(tok -> {
                try {
                    return Optional.of(tok.nextValue());
                }
                /* XXX Should we instead fake up an error response?
                 * Possibly a 6XX error code (client-side errors)? */
                catch (JSONException e) {
                    log.error("Error parsing JSON: {}", e.toString());
                    return Optional.<Object>empty();
                }
            });
    }

    public SimpleHttpResponse getResponse () { return response; }
    public int getCode () { return response.getCode(); }

    public boolean ok ()
    {
        int code = getCode();
        return code >= 200 && code < 300;
    }
    public Optional<JsonResponse> ifOk ()
    {
        return ok() ? Optional.of(this) : Optional.<JsonResponse>empty();
    }

    public Optional<String> getHeader (String name)
        throws ProtocolException
    {
        return Optional.ofNullable(response.getHeader(name))
            .map(h -> h.getValue());
    }

    public Optional<Object> getBody () { return body; }
    public boolean hasBody () { return body.isPresent(); }

    /* Java 11 sucks
    public <T> Optional<T> getBodyAs ()
    {
        return body
            .filter(o -> o instanceof T)
            .map(o -> (T)o);
    }
    */

    public Optional<JSONObject> getBodyObject ()
    {
        return body
            .filter(o -> o instanceof JSONObject)
            .map(o -> (JSONObject)o);
    }
    public Optional<JSONArray> getBodyArray ()
    {
        return body
            .filter(o -> o instanceof JSONArray)
            .map(o -> (JSONArray)o);
    }
}
