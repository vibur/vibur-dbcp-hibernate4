/**
 * Copyright 2013 Simeon Malchev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.vibur.dbcp.integration;

import org.hibernate.cfg.Environment;
import org.hibernate.service.UnknownUnwrapTypeException;
import org.hibernate.service.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Stoppable;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * <p>A connection provider for Hibernate org.vibur.dbcp.integration.
 *
 * <p>To use this connection provider set:<br/>
 * <code>hibernate.connection.provider_class&nbsp;org.vibur.dbcp.org.vibur.dbcp.integration.ViburDBCPConnectionProvider</code>
 *
 * <pre>
 * Supported Hibernate properties:<br/>
 *   hibernate.connection.driver_class
 *   hibernate.connection.url
 *   hibernate.connection.username
 *   hibernate.connection.password
 *   hibernate.connection.isolation
 *   hibernate.connection.autocommit
 * </pre>
 *
 * All {@link org.vibur.dbcp.ViburDBCPConfig} properties are also supported via using the
 * {@code hibernate.vibur} prefix.
 *
 * @see ConnectionProvider
 *
 * @author Simeon Malchev
 */
public class ViburDBCPConnectionProvider implements ConnectionProvider, Configurable, Stoppable {

    private static final String VIBUR_PREFIX = "hibernate.vibur.";

    private ViburDBCPDataSource dataSource = null;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    public void configure(Map configurationValues) {
        dataSource = new ViburDBCPDataSource(transform(configurationValues));
        dataSource.start();
    }

    /** {@inheritDoc} */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /** {@inheritDoc} */
    public void closeConnection(Connection conn) throws SQLException {
        conn.close();
    }

    /** {@inheritDoc} */
    public void stop() {
        if (dataSource != null) {
            dataSource.terminate();
            dataSource = null;
        }
    }

    /** {@inheritDoc} */
    public boolean supportsAggressiveRelease() {
        return false;
    }

    /** {@inheritDoc} */
    public boolean isUnwrappableAs(Class unwrapType) {
        return ConnectionProvider.class.equals(unwrapType) ||
            ViburDBCPConnectionProvider.class.isAssignableFrom(unwrapType);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    public <T> T unwrap(Class<T> unwrapType) {
        if (isUnwrappableAs(unwrapType))
            return (T) this;
        else
            throw new UnknownUnwrapTypeException(unwrapType);
    }

    private Properties transform(Map<String, String> configurationValues) {
        Properties result  = new Properties();

        String driverClassName = configurationValues.get(Environment.DRIVER);
        if (driverClassName != null)
            result.setProperty("driverClassName", driverClassName);
        String jdbcUrl = configurationValues.get(Environment.URL);
        if (jdbcUrl != null)
            result.setProperty("jdbcUrl", jdbcUrl);

        String username = configurationValues.get(Environment.USER);
        if (username != null)
            result.setProperty("username", username);
        String password = configurationValues.get(Environment.PASS);
        if (password != null)
            result.setProperty("password", password);

        String defaultTransactionIsolationValue = configurationValues.get(Environment.ISOLATION);
        if (defaultTransactionIsolationValue != null)
            result.setProperty("defaultTransactionIsolationValue", defaultTransactionIsolationValue);
        String defaultAutoCommit = configurationValues.get(Environment.AUTOCOMMIT);
        if (defaultAutoCommit != null)
            result.setProperty("defaultAutoCommit", defaultAutoCommit);

        for (Map.Entry<String, String> entry : configurationValues.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(VIBUR_PREFIX)) {
                key = key.substring(VIBUR_PREFIX.length());
                result.setProperty(key, entry.getValue());
            }
        }

        return result;
    }

    public ViburDBCPDataSource getDataSource() {
        return dataSource;
    }
}
