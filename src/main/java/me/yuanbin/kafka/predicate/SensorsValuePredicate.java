package me.yuanbin.kafka.predicate;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;

public class SensorsValuePredicate implements Predicate<String, JsonNode> {

    private static final String PROPERTIES = "properties";
    private List<String> whitelistPropertyFields;

    public SensorsValuePredicate(List<String> whitelistPropertyFields) {
        this.whitelistPropertyFields = whitelistPropertyFields;
    }

    public boolean test(String key, JsonNode value) {
        if (value == null || !value.hasNonNull(PROPERTIES)) {
            return false;
        }
        JsonNode properties = value.get(PROPERTIES);
        // properties should have all the whitelist fields
        for (String field : whitelistPropertyFields) {
            if (!properties.has(field)) return false;
        }
        return true;
    }
}
