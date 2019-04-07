package me.yuanbin.kafka.predicate;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class MaxwellKeyPredicate implements Predicate<JsonNode, byte[]> {

    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private Set<String> whitelistDbTable;
    private boolean regexMode;
    private Pattern pattern;

    public MaxwellKeyPredicate(List<String> whitelistDbTable, boolean regexMode) {
        this.whitelistDbTable = new HashSet<>(whitelistDbTable);
        this.regexMode = regexMode;
        if (regexMode) {
            pattern = Pattern.compile(String.join("|", this.whitelistDbTable));
        }
    }

    public MaxwellKeyPredicate(List<String> whitelistDbTable) {
        this(whitelistDbTable, false);
    }

    public boolean test(JsonNode key, byte[] value) {
        if (key == null || !key.hasNonNull(DATABASE) || !key.hasNonNull(TABLE)) {
            return false;
        }
        String database = key.get(DATABASE).asText();
        String table = key.get(TABLE).asText();
        String dbTable = String.format("%s.%s", database, table);
        if (regexMode) {
            return pattern.matcher(dbTable).matches();
        } else {
            String dbAllTable = String.format("%s.%s", database, "*");
            return whitelistDbTable.contains(dbTable) || whitelistDbTable.contains(dbAllTable);
        }
    }
}
