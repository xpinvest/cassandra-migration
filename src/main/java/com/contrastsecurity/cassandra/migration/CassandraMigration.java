package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.action.Initialize;
import com.contrastsecurity.cassandra.migration.action.Migrate;
import com.contrastsecurity.cassandra.migration.action.Validate;
import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.config.MigrationConfigs;
import com.contrastsecurity.cassandra.migration.config.ScriptsLocations;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.contrastsecurity.cassandra.migration.info.MigrationInfoService;
import com.contrastsecurity.cassandra.migration.info.MigrationVersion;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.resolver.CompositeMigrationResolver;
import com.contrastsecurity.cassandra.migration.resolver.MigrationResolver;
import com.contrastsecurity.cassandra.migration.utils.VersionPrinter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import java.util.List;
import javax.annotation.Nullable;

public class CassandraMigration {

    private static final Log LOG = LogFactory.getLog(CassandraMigration.class);

    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    private Keyspace keyspace;
    private MigrationConfigs configs;

    public CassandraMigration() {
        this.keyspace = new Keyspace();
        this.configs = new MigrationConfigs();
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Sets the ClassLoader to use for resolving migrations on the classpath.
     *
     * @param classLoader The ClassLoader to use for resolving migrations on the classpath. (default: Thread.currentThread().getContextClassLoader() )
     */
    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    public MigrationConfigs getConfigs() {
        return configs;
    }

    private MigrationResolver createMigrationResolver() {
        return new CompositeMigrationResolver(classLoader, new ScriptsLocations(configs.getScriptsLocations()), configs.getEncoding());
    }

    public int migrate() {
        return execute((Session session) -> {
            new Initialize().run(session, keyspace, MigrationVersion.CURRENT.getTable());
            
            MigrationResolver migrationResolver = createMigrationResolver();
            SchemaVersionDAO schemaVersionDAO = createSchemaVersionDao(session);
            Migrate migrate = new Migrate
                    ( migrationResolver
                    , configs.getTarget()
                    , schemaVersionDAO
                    , session
                    , keyspace.getCluster().getUsername()
                    , configs.isAllowOutOfOrder());
            
            return migrate.run();
        });
    }

    public MigrationInfoService info() {
        return execute((Session session) -> {
            MigrationResolver migrationResolver = createMigrationResolver();
            SchemaVersionDAO schemaVersionDAO = createSchemaVersionDao(session);
            MigrationInfoService migrationInfoService = new MigrationInfoService
                                   (migrationResolver, schemaVersionDAO, configs.getTarget(), false, true);
            migrationInfoService.refresh();
            
            return migrationInfoService;
        });
    }

    private SchemaVersionDAO createSchemaVersionDao(Session session) {
        return new SchemaVersionDAO(session, keyspace, MigrationVersion.CURRENT.getTable());
    }

    public void validate() {
    	String validationError = execute((Session session) -> {
            MigrationResolver migrationResolver = createMigrationResolver();
            SchemaVersionDAO schemaVersionDao = createSchemaVersionDao(session);
            Validate validate = new Validate(migrationResolver, schemaVersionDao, configs.getTarget(), true, false);
            return validate.run();
        });
    
    	if (validationError != null) {
    		throw new CassandraMigrationException("Validation failed. " + validationError);
    	}
    }
    
    public void baseline() {
        throw new UnsupportedOperationException("TODO");
    }

    private String getConnectionInfo(Metadata metadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("Connected to cluster: ");
        sb.append(metadata.getClusterName());
        sb.append("\n");
        for (Host host : metadata.getAllHosts()) {
            sb.append("Data center: ");
            sb.append(host.getDatacenter());
            sb.append("; Host: ");
            sb.append(host.getAddress());
        }
        return sb.toString();
    }

    <T> T execute(Action<T> action) {
        T result;

        VersionPrinter.printVersion(classLoader);

        com.datastax.driver.core.Cluster cluster = null;
        Session session = null;
        try {
            if (null == keyspace)
                throw new IllegalArgumentException("Unable to establish Cassandra session. Keyspace is not configured.");

            if (null == keyspace.getCluster())
                throw new IllegalArgumentException("Unable to establish Cassandra session. Cluster is not configured.");

            cluster = createCassandraCluster();

            Metadata metadata = cluster.getMetadata();
            LOG.info(getConnectionInfo(metadata));

            session = cluster.newSession();
            if (keyspace.getName() == null
            ||  keyspace.getName().trim().isEmpty())
                throw new IllegalArgumentException("Keyspace not specified.");
            
            List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces();
            boolean keyspaceExists = false;
            for (KeyspaceMetadata keyspaceMetadata : keyspaces) {
                if (keyspaceMetadata.getName().equalsIgnoreCase(keyspace.getName()))
                    keyspaceExists = true;
            }
            
            if (keyspaceExists) {
                session.execute("USE " + keyspace.getName());
            } else {
                throw new CassandraMigrationException("Keyspace: " + keyspace.getName() + " does not exist.");
            }

            result = action.execute(session);
        } finally {
            closeSession(session);
            closeCluster(cluster);
        }
        return result;
    }

    /** Close the cluster */
    protected void closeCluster(@Nullable Cluster cluster) {
        if (null != cluster && !cluster.isClosed()) {
            try {
                cluster.close();
            } catch (Exception e) {
                LOG.warn("Error closing Cassandra cluster");
            }
        }
    }

    /** Close the session */
    protected void closeSession(@Nullable Session session) {
        if (null != session && !session.isClosed()) {
            try {
                session.close();
            } catch(Exception e) {
                LOG.warn("Error closing Cassandra session");
            }
        }
    }

    protected Cluster createCassandraCluster() throws IllegalArgumentException {
        return createBuilder().build();
    }

    private Cluster.Builder createBuilder() throws IllegalArgumentException {
        com.datastax.driver.core.Cluster.Builder result = new com.datastax.driver.core.Cluster.Builder();
        result.addContactPoints(keyspace.getCluster().getContactpoints()).withPort(keyspace.getCluster().getPort());
        if (keyspace.getCluster().getUsername() != null
        && !keyspace.getCluster().getUsername().trim().isEmpty()) {
            if (keyspace.getCluster().getPassword() != null
            && !keyspace.getCluster().getPassword().trim().isEmpty()) {
                result.withCredentials(
                        keyspace.getCluster().getUsername(),
                        keyspace.getCluster().getPassword());
            } else {
                throw new IllegalArgumentException("Password must be provided with username.");
            }
        }
        return result;
    }

    interface Action<T> {
        T execute(Session session);
    }
}
