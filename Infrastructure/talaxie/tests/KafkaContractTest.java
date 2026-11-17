import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import routines.AbAssuranceKafka;

public class KafkaContractTest {
    private static void same(Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected " + expected + ", received " + actual);
        }
    }

    private static void rejected(Runnable operation) {
        try {
            operation.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError("Invalid input was accepted");
    }

    public static void main(String[] args) {
        same("12345678901234567890.1200", AbAssuranceKafka.jsonValue(new BigDecimal("12345678901234567890.1200")));
        same("1990-05-12", AbAssuranceKafka.jsonValue(java.sql.Date.valueOf("1990-05-12")));
        same("2025-03-10T14:30:12.123456", AbAssuranceKafka.jsonValue(Timestamp.valueOf("2025-03-10 14:30:12.123456")));
        if (AbAssuranceKafka.jsonValue(null) != null) throw new AssertionError("NULL was lost");
        same(Long.valueOf(9007199254740993L), AbAssuranceKafka.jsonValue(Long.valueOf(9007199254740993L)));

        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("name", "Élodie \"Dupont\"\nParis");
        row.put("amount", AbAssuranceKafka.jsonValue(new BigDecimal("100.20")));
        row.put("optional", null);
        Gson json = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
        same("9007199254740993", json.toJson(AbAssuranceKafka.jsonValue(Long.valueOf(9007199254740993L))));
        Map<?, ?> restored = json.fromJson(json.toJson(row), Map.class);
        same(row.get("name"), restored.get("name"));
        same("100.20", restored.get("amount"));
        if (!restored.containsKey("optional")) throw new AssertionError("NULL column omitted");

        String[] entities = {"customers", "contracts", "claims", "payments"};
        String[] oracle = {"AB_CLIENT_ID", "AB_POLICY_NUMBER", "AB_CLAIM_ID", "AB_PAYMENT_ID"};
        String[] sqlserver = {"AP_USER_ID", "AP_CONTRACT_REF", "AP_SINISTRE_NUM", "AP_PAYMENT_REF"};
        for (int index = 0; index < entities.length; index++) {
            same(oracle[index], AbAssuranceKafka.primaryKey("abassurance", entities[index]));
            same(sqlserver[index], AbAssuranceKafka.primaryKey("assureplus", entities[index]));
        }
        rejected(() -> AbAssuranceKafka.primaryKey("unknown", "customers"));
        rejected(() -> AbAssuranceKafka.primaryKey("abassurance", "unknown"));
        rejected(() -> AbAssuranceKafka.jsonValue(Double.NaN));
        rejected(() -> AbAssuranceKafka.jsonValue(new Object()));
        System.out.println("Kafka source contract: OK");
    }
}
