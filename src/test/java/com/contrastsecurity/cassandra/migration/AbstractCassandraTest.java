package com.contrastsecurity.cassandra.migration;

import ch.qos.logback.core.util.Duration;
import com.datastax.driver.core.Cluster.Builder;
import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractCassandraTest {
    
    private static final Duration TIMEOUT = Duration.buildByMinutes(10);
    
    public static final String CASSANDRA__KEYSPACE = "cassandra_migration_test";
    public static final String CASSANDRA_CONTACT_POINT = "localhost";
    public static final int CASSANDRA_PORT = 9147;
    public static final String CASSANDRA_USERNAME = "cassandra";
    public static final String CASSANDRA_PASSWORD = "cassandra";

    private Session session;

    @BeforeClass
    public static void beforeSuite() throws Exception {
        setupCassandraStorageDir();
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(
                "/cassandra-unit.yaml", // Only a root is allowed for tests?
                "target/embeddedCassandra",
                TIMEOUT.getMilliseconds());
    }

    /** Set before creating CassandraCQLUnit or set the configuration: 
     * File cassandra-unit.yamp -> cdc_raw_directory: target/embeddedCassandra/cdc
     */
    private static void setupCassandraStorageDir() {
        if (!false) {
            String dir = Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("")
                    .getFile() 
                    + "cassandra-" 
                    + System.nanoTime();

            System.setProperty("cassandra.storagedir", dir);            
        }
    }

    @AfterClass
    public static void afterSuite() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Before
    public void createKeyspace() {
        Statement statement = new SimpleStatement(
                "CREATE KEYSPACE " + CASSANDRA__KEYSPACE +
                        "  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
        );
        getSession(getKeyspace()).execute(statement);
    }

    @After
    public void dropKeyspace() {
        Statement statement = new SimpleStatement(
                "DROP KEYSPACE " + CASSANDRA__KEYSPACE + ";"
        );
        getSession(getKeyspace()).execute(statement);
    }

    protected Keyspace getKeyspace() {
        Keyspace ks = new Keyspace();
        ks.setName(CASSANDRA__KEYSPACE);
        ks.getCluster().setContactpoints(CASSANDRA_CONTACT_POINT);
        ks.getCluster().setPort(CASSANDRA_PORT);
        ks.getCluster().setUsername(CASSANDRA_USERNAME);
        ks.getCluster().setPassword(CASSANDRA_PASSWORD);
        return ks;
    }

    private Session getSession(Keyspace keyspace) {
        if (session != null && !session.isClosed())
            return session;
        
        Builder builder = new Builder()
                .addContactPoints(CASSANDRA_CONTACT_POINT)
                .withPort(CASSANDRA_PORT)
                .withSocketOptions(createSocketOption(TIMEOUT))
                ;
        builder.withCredentials
                ( keyspace.getCluster().getUsername()
                , keyspace.getCluster().getPassword());
        Cluster cluster = builder.build();
        session = cluster.connect();
        return session;
    }

    /** Configure socket options */
    private SocketOptions createSocketOption(Duration timeout) {
        SocketOptions options = new SocketOptions();
        options.setConnectTimeoutMillis((int) timeout.getMilliseconds());
        options.setReadTimeoutMillis((int) timeout.getMilliseconds());
        options.setTcpNoDelay(true);
        return options;
    }
    
    protected Session getSession() {
        return session;
    }
}
