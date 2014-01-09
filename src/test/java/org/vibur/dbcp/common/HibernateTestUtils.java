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

package org.vibur.dbcp.common;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.vibur.dbcp.model.Actor;


/**
 * Simple Hibernate utils class.
 *
 * @author Simeon Malchev
 */
public class HibernateTestUtils {

    private static SessionFactory sessionFactoryNoStmtCache;
    private static SessionFactory sessionFactoryWithStmtCache;

    static {
        sessionFactoryNoStmtCache = buildSessionFactory("hibernate-no-stmt-cache.cfg.xml");
        sessionFactoryWithStmtCache = buildSessionFactory("hibernate-with-stmt-cache.cfg.xml");
    }

    private static SessionFactory buildSessionFactory(String configFileName) {
        try {
            Configuration cfg = new Configuration().configure(configFileName);
            cfg.addAnnotatedClass(Actor.class);
            ServiceRegistry serviceRegistry =
                new ServiceRegistryBuilder().applySettings(cfg.getProperties()).buildServiceRegistry();
            return cfg.buildSessionFactory(serviceRegistry);
        } catch (HibernateException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static SessionFactory getSessionFactoryWithoutStmtCache() {
        return sessionFactoryNoStmtCache;
    }

    public static SessionFactory getSessionFactoryWithStmtCache() {
        return sessionFactoryWithStmtCache;
    }
}
