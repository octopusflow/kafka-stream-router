package me.yuanbin.kafka.predicate;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.regex.Pattern;

public class MaxwellKeyPredicate implements Predicate<JsonNode, byte[]> {

    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private final Pattern pattern;

    public MaxwellKeyPredicate(String dbRegex) {
        this.pattern = Pattern.compile(dbRegex);
    }

    public boolean test(JsonNode key, byte[] value) {
        if (key == null || !key.hasNonNull(DATABASE) || !key.hasNonNull(TABLE)) {
            return false;
        }
        String database = key.get(DATABASE).asText();
        String table = key.get(TABLE).asText();
        String dbTable = database + "." + table;
        return pattern.matcher(dbTable).matches();
    }
}
