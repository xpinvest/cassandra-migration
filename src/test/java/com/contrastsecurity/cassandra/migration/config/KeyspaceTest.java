package com.contrastsecurity.cassandra.migration.config;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class KeyspaceTest {
    @Test
    public void shouldDefaultToNoKeyspaceButCanBeOverridden() {
        assertThat(new KeyspaceConfig().getName(), is(nullValue()));
        System.setProperty(KeyspaceConfig.KeyspaceProperty.NAME.getName(), "myspace");
        assertThat(new KeyspaceConfig().getName(), is("myspace"));
    }

    @Test
    public void shouldHaveDefaultClusterObject() {
        assertThat(new KeyspaceConfig().getCluster(), is(notNullValue()));
    }
}
