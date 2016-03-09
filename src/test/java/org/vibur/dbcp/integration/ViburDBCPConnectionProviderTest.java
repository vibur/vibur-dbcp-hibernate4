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

import org.hibernate.Session;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hsqldb.cmdline.SqlToolError;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.runners.MockitoJUnitRunner;
import org.vibur.dbcp.ViburDBCPDataSource;
import org.vibur.dbcp.cache.ConnMethodKey;
import org.vibur.dbcp.cache.StatementVal;
import org.vibur.dbcp.model.Actor;
import org.vibur.dbcp.util.HibernateTestUtils;
import org.vibur.dbcp.util.HsqldbUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.inOrder;
import static org.vibur.dbcp.cache.StatementVal.AVAILABLE;
import static org.vibur.dbcp.util.StatementCacheUtils.mockStatementCache;

/**
 * Hibernate unit/integration test.
 *
 * @author Simeon Malchev
 */
@RunWith(MockitoJUnitRunner.class)
public class ViburDBCPConnectionProviderTest {

    @BeforeClass
    public static void deployDatabaseSchemaAndData() throws IOException, SqlToolError, SQLException {
        Properties properties = ((SessionFactoryImplementor)
            HibernateTestUtils.getSessionFactoryWithStmtCache()).getProperties();
        HsqldbUtils.deployDatabaseSchemaAndData(properties.getProperty("hibernate.connection.url"),
            properties.getProperty("hibernate.connection.username"),
            properties.getProperty("hibernate.connection.password"));
    }

    @Captor
    private ArgumentCaptor<ConnMethodKey> key1, key2;
    @Captor
    private ArgumentCaptor<StatementVal> val1;

    @Test
    public void testSelectStatementNoStatementsCache() throws SQLException {
        Session session = HibernateTestUtils.getSessionFactoryWithoutStmtCache().getCurrentSession();
        try {
            executeAndVerifySelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        }
    }

    @Test
    public void testSelectStatementWithStatementsCache() throws SQLException {
        Session session = HibernateTestUtils.getSessionFactoryWithStmtCache().openSession();

        ConnectionProvider cp = ((SessionFactoryImplementor) session.getSessionFactory())
                .getServiceRegistry().getService(ConnectionProvider.class);
        ViburDBCPDataSource ds = ((ViburDBCPConnectionProvider) cp).getDataSource();

        ConcurrentMap<ConnMethodKey, StatementVal> mockedStatementCache = mockStatementCache(ds);

        executeAndVerifySelectInSession(session);
        // resources/hibernate-with-stmt-cache.cfg.xml defines pool with 1 connection only, that's why
        // the second session will get and use the same underlying connection.
        session = HibernateTestUtils.getSessionFactoryWithStmtCache().openSession();
        executeAndVerifySelectInSession(session);

        InOrder inOrder = inOrder(mockedStatementCache);
        inOrder.verify(mockedStatementCache).get(key1.capture());
        inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), val1.capture());
        inOrder.verify(mockedStatementCache).get(key2.capture());

        assertEquals(1, mockedStatementCache.size());
        assertTrue(mockedStatementCache.containsKey(key1.getValue()));
        assertEquals(key1.getValue(), key2.getValue());
        assertEquals("prepareStatement", key1.getValue().getMethod().getName());
        assertEquals(AVAILABLE, val1.getValue().state().get());
    }

    private void executeAndVerifySelectInSession(Session session) {
        try {
            executeAndVerifySelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    @SuppressWarnings("unchecked")
    private void executeAndVerifySelect(Session session) {
        session.beginTransaction();
        List<Actor> list = session.createQuery("from Actor where firstName = ?")
            .setParameter(0, "CHRISTIAN").list();
        session.getTransaction().commit();

        Set<String> expectedLastNames = new HashSet<>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
        assertEquals(expectedLastNames.size(), list.size());
        for (Actor actor : list) {
            assertTrue(expectedLastNames.remove(actor.getLastName()));
        }
    }
}
