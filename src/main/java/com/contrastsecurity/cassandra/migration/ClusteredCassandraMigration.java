package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.Cluster;

/**
 * An extended CassandraMigration 
 * @author Pavel Ponec
 */
public class ClusteredCassandraMigration extends CassandraMigration {
    
    private static final Log LOG = LogFactory.getLog(ClusteredCassandraMigration.class);    
    
    private final Cluster cluster;

    public ClusteredCassandraMigration(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    protected Cluster createCluster() throws IllegalArgumentException {
        return cluster;
    }

    @Override
    protected void closeCluster(Cluster cluster) {
        LOG.info("Dummy closing the cluster");
    }       
    
}
