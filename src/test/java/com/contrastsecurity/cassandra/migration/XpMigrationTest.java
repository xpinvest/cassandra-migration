package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.config.KeyspaceConfig;
import com.datastax.driver.core.Session;
import org.junit.Test;

public class XpMigrationTest extends AbstractCassandraTest {

    /**
     * Test of init method, of class FlywayServiceImpl.
     */
    @Test
    public void testAfterInit() {
        migrate();

        // TODO: Make a test:
        Session session = super.getSession();
    }

    /**
     * Test of init method, of class FlywayServiceImpl.
     */
    public void migrate() {
        String[] scriptsLocations = {
            "migration/integ", 
            "migration/integ/java"};

        KeyspaceConfig keyspace = getKeyspace();
        CassandraMigration cm = new CassandraMigration();
        cm.getConfigs().setScriptsLocations(scriptsLocations);
        cm.setKeyspace(keyspace);
        cm.migrate();
    }


}
