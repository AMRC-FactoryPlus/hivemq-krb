/* Factory+ HiveMQ auth plugin.
 * MQTT ACE class.
 * Copyright 2022 AMRC.
 */

package uk.co.amrc.factoryplus.hivemq_auth_krb;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.regex.*;

import org.json.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.extension.sdk.api.auth.parameter.*;
import com.hivemq.extension.sdk.api.services.builder.Builders;

class MqttAce {
    private static final Logger log = LoggerFactory.getLogger(MqttAce.class);

    public static final WANT_TARG  = 0x1;
    public static final WANT_PRINC = 0x2;

    private static final Pattern TEMPLATES = Pattern.compile("%([gndGN])")
    private static final String PRINC_TEMPLS = "GN";

    private UUID target;
    private String template;
    private TopicPermission.MqttActivity activity;
    private Optional<Matcher> matcher;
    private int want;

    public static Optional<MqttAce> expandEntry (
        Map.Entry<String, Object> entry, UUID target)
    {
        return Optional.ofNullable(entry.getValue())
            .filter(v -> v instanceof String)
            .map(v -> expandActivity((String)v))
            .map(act -> new MqttAce(entry.getKey(), act, target));
    }

    public UUID getTarget () { return target; }

    public boolean uses (int check)
    {
        return (want & check) != 0;
    }

    public Optional<TopicPermission> toTopicPermission (
        JSONObject target, JSONObject principal)
    {
        String topic = expandTopic(target, principal);
        return Builders.topicPermission()
            .activity(activity)
            .topicFilter(topic)
            .type(TopicPermission.PermissionType.ALLOW)
            .build();
    }

    private MqttAce (String tmpl, TopicPermission.MqttActivity act, UUID target)
    {
        template = tmpl;
        activity = act;

        matcher = TEMPLATES.matcher(template);
        want = matcher.results()
            .map(m -> PRINC_TEMPLS.contains(m.group(1))
                ? WANT_PRINC : WANT_TARG)
            .reduce(0, (a, b) -> a | b);
    }

    /* Please Sir, can I have typedef? */
    private static TopicPermission.MqttActivity expandAccess (String rw)
    {
        if (rw.contains("r")) {
            if (rw.contains("w"))
                return TopicPermission.MqttActivity.ALL;
            return TopicPermission.MqttActivity.SUBSCRIBE;
        }
        else {
            if (rw.contains("w"))
                return TopicPermission.MqttActivity.PUBLISH;
        }
        return null;
    }

    private static String expandTemplate (String tmpl,
        JSONObject target, JSONObject principal)
    {
        JSONObject addr = PRINC_TEMPS.contains(tmpl) ? principal : target;

        String rv = null;
        switch (tmpl) {
            case "g":
            case "G":
                rv = addr.getString("group_id");
                break;
            case "n":
            case "N":
                rv = addr.getString("node_id");
                break;
            case "d":
                rv = addr.getString("device_id");
                break
        }

        return Matcher.quoteReplacement(rv);
    }

    private String expandTopic (JSONObject target, JSONObject principal)
    {
        if (want == 0)
            return template;

        return matcher.replaceAll(match -> 
            expandTemplate(match.group(1), target, principal));
    }
}

