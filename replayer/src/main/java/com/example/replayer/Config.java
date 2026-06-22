package com.example.replayer;

/** Runtime configuration, all overridable via environment variables. */
final class Config {
    final String dataDir;
    final String dbUrl;
    final String dbUser;
    final String dbPassword;
    final long speedup;       // real-time millis are divided by this to get sleep millis
    final long maxSleepMs;    // cap on sleep between consecutive events
    final int limitOrders;    // 0 = all orders
    final boolean truncateFirst;

    private Config(String dataDir, String dbUrl, String dbUser, String dbPassword,
                   long speedup, long maxSleepMs, int limitOrders, boolean truncateFirst) {
        this.dataDir = dataDir;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.speedup = Math.max(1, speedup);
        this.maxSleepMs = Math.max(0, maxSleepMs);
        this.limitOrders = limitOrders;
        this.truncateFirst = truncateFirst;
    }

    static Config fromEnv() {
        return new Config(
                env("DATA_DIR", "/data"),
                env("DB_URL", "jdbc:mysql://mysql-source:3306/olist"
                        + "?rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"),
                env("DB_USER", "app"),
                env("DB_PASSWORD", "app"),
                Long.parseLong(env("REPLAY_SPEEDUP", "120000")),
                Long.parseLong(env("REPLAY_MAX_SLEEP_MS", "200")),
                Integer.parseInt(env("REPLAY_LIMIT_ORDERS", "0")),
                Boolean.parseBoolean(env("REPLAY_TRUNCATE_FIRST", "true")));
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
