package sqlancer.dbms;

public class TestConfig {
    public static final String NUM_QUERIES = "1000";
    public static final String SECONDS = "300";

    public static final String CLICKHOUSE_ENV = "CLICKHOUSE_AVAILABLE";
    public static final String COCKROACHDB_ENV = "COCKROACHDB_AVAILABLE";
    public static final String DATABEND_ENV = "DATABEND_AVAILABLE";
    public static final String DATAFUSION_ENV = "DATAFUSION_AVAILABLE";
    public static final String DORIS_ENV = "DORIS_AVAILABLE";
    public static final String MARIADB_ENV = "MARIADB_AVAILABLE";
    public static final String MATERIALIZE_ENV = "MATERIALIZE_AVAILABLE";
    public static final String POSTGRES_ENV = "POSTGRES_AVAILABLE";
    public static final String YUGABYTE_ENV = "YUGABYTE_AVAILABLE";

    public static boolean isEnvironmentTrue(String key) {
        String value = System.getenv(key);
        return value != null && value.equalsIgnoreCase("true");
    }
}
