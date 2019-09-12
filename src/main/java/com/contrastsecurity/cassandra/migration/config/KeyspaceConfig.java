package com.contrastsecurity.cassandra.migration.config;

public class KeyspaceConfig {
    private static final String PROPERTY_PREFIX = "cassandra.migration.keyspace.";
    private static final String ENV_PREFIX = "CASSANDRA_MIGRATION_KEYSPACE_";

    public enum KeyspaceProperty {
        /** Singleton */
        NAME(PROPERTY_PREFIX + "name", ENV_PREFIX + "NAME", "Name of Cassandra keyspace");

        private final String name;
        private final String envName;
        private final String description;

        KeyspaceProperty(String name, String envName, String description) {
            this.name = name;
            this.envName = envName;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getEnvName() {
            return envName;
        }

        public String getDescription() {
            return description;
        }
    }

    private ClusterConfig cluster;
    private String name;

    public KeyspaceConfig() {
        cluster = new ClusterConfig();
        String keyspaceP = PropertyGetter.getProperty(KeyspaceProperty.NAME.getName(), KeyspaceProperty.NAME.getEnvName());
        
        if (keyspaceP != null
        && !keyspaceP.trim().isEmpty())
            this.name = keyspaceP;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public void setCluster(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
