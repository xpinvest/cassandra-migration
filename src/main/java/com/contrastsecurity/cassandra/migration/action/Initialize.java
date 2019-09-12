package com.contrastsecurity.cassandra.migration.action;

import com.contrastsecurity.cassandra.migration.config.KeyspaceConfig;
import com.contrastsecurity.cassandra.migration.dao.SchemaVersionDAO;
import com.datastax.driver.core.Session;

public class Initialize {

    public void run(Session session, KeyspaceConfig keyspace, String migrationVersionTableName) {
        SchemaVersionDAO dao = new SchemaVersionDAO(session, keyspace, migrationVersionTableName);
        dao.createTablesIfNotExist();
    }
}
