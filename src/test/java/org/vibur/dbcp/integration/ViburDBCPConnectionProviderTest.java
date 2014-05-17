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
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hsqldb.cmdline.SqlToolError;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.runners.MockitoJUnitRunner;
import org.vibur.dbcp.ViburDBCPDataSource;
import org.vibur.dbcp.cache.MethodDef;
import org.vibur.dbcp.cache.ReturnVal;
import org.vibur.dbcp.util.HibernateTestUtils;
import org.vibur.dbcp.util.HsqldbUtils;
import org.vibur.dbcp.model.Actor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * Simple Hibernate unit/integration test.
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
    private ArgumentCaptor<MethodDef<Connection>> key1, key2;

    @Test
    public void testSimpleSelectStatementNoStatementsCache() throws SQLException {
        Session session = HibernateTestUtils.getSessionFactoryWithoutStmtCache().getCurrentSession();
        try {
            executeAndVerifySimpleSelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleSelectStatementWithStatementsCache() throws SQLException {
        Session session = HibernateTestUtils.getSessionFactoryWithStmtCache().openSession();

        ConnectionProvider cp = ((SessionFactoryImplementor) session.getSessionFactory()).getConnectionProvider();
        ViburDBCPDataSource ds = ((ViburDBCPConnectionProvider) cp).getDataSource();

        ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> mockedStatementCache =
            mock(ConcurrentMap.class, delegatesTo(ds.getStatementCache()));
        ds.setStatementCache(mockedStatementCache);

        executeAndVerifySimpleSelectInSession(session);
        // resources/hibernate-with-stmt-cache.cfg.xml defines pool with 1 connection only, that's why
        // the second session will get and use the same underlying connection.
        session = HibernateTestUtils.getSessionFactoryWithStmtCache().openSession();
        executeAndVerifySimpleSelectInSession(session);

        InOrder inOrder = inOrder(mockedStatementCache);
        inOrder.verify(mockedStatementCache).get(key1.capture());
        inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), any(ReturnVal.class));
        inOrder.verify(mockedStatementCache).get(key2.capture());

        assertEquals(key1.getValue(), key2.getValue());
        assertEquals("prepareStatement", key1.getValue().getMethod().getName());
        ReturnVal<Statement> valueHolder = mockedStatementCache.get(key1.getValue());
        assertFalse(valueHolder.inUse().get());
    }

    private void executeAndVerifySimpleSelectInSession(Session session) {
        try {
            executeAndVerifySimpleSelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    @SuppressWarnings("unchecked")
    private void executeAndVerifySimpleSelect(Session session) {
        session.beginTransaction();
        List<Actor> list = session.createQuery("from Actor where firstName = ?")
            .setParameter(0, "CHRISTIAN").list();
        session.getTransaction().commit();

        Set<String> expectedLastNames = new HashSet<String>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
        assertEquals(expectedLastNames.size(), list.size());
        for (Actor actor : list) {
            assertTrue(expectedLastNames.remove(actor.getLastName()));
        }
    }
}
