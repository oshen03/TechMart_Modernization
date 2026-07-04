package com.techmart.util;

import jakarta.annotation.Resource;
import jakarta.ejb.Stateless;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Centralises JNDI DataSource access for all EJBs.
 *
 * The @Resource annotation instructs the EJB container to inject the
 * pooled DataSource configured in glassfish-resources.xml at deployment
 * time — eliminating brittle hard-coded JNDI lookups and giving the
 * container full control over connection lifecycle.
 *
 * The DataSource is configured as a GlassFish connection pool with:
 *   min-pool-size=5, max-pool-size=30, idle-timeout=300s
 *
 * JNDI name: jdbc/TechMartDS  (defined in glassfish-resources.xml)
 */
@Stateless
public class DataSourceProvider {

    private static final Logger LOG = Logger.getLogger(DataSourceProvider.class.getName());

    /** Container injects the pooled DataSource via JNDI at startup. */
    @Resource(lookup = "jdbc/TechMartDS")
    private DataSource dataSource;

    /**
     * Borrows a connection from the pool.
     * Callers must close() the connection (returns it to pool, not DB).
     */
    public Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new IllegalStateException(
                "DataSource not injected — verify jdbc/TechMartDS is configured in GlassFish.");
        }
        Connection conn = dataSource.getConnection();
        LOG.fine("Connection borrowed from pool: " + conn);
        return conn;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
