package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Test;

public class XpMigrationTest extends BaseIT {

    private final Class xx = EmbeddedCassandraServerHelper.class;

    /**
     * Test of init method, of class FlywayServiceImpl.
     */
    @Test
    public void testAfterInit() {
        migrate();

        Session session = super.getSession();
    }

    /**
     * Test of init method, of class FlywayServiceImpl.
     */
    public void migrate() {
        String[] scriptsLocations = {"migration/integ", "migration/integ/java"};

        Keyspace keyspace = getKeyspace();
        CassandraMigration cm = new CassandraMigration();
        cm.getConfigs().setScriptsLocations(scriptsLocations);
        cm.setKeyspace(keyspace);
        cm.migrate();
    }


}