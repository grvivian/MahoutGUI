package br.com.grvivian.hibernate;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

/**
 *
 * @author glaucio
 */
public class HibernateUtil {

  private static SessionFactory sessionFactory = null;

  public static SessionFactory getSessionFactory(ConfigDB configDB) throws Exception {
    if (sessionFactory != null) {
      return sessionFactory;
    }

    if (configDB == null) {
      return null;
    }

    Configuration cfg = new Configuration();

    switch (configDB.getTpDB()) {
      case MYSQL:
        cfg.setProperty(Environment.DIALECT, org.hibernate.dialect.MySQLInnoDBDialect.class.getName());
        cfg.setProperty(Environment.DRIVER, com.mysql.jdbc.Driver.class.getName());
        cfg.setProperty(Environment.URL, "jdbc:mysql://" + configDB.getHost() + ":" + configDB.getPort() + "/" + configDB.getDatabase());
        break;
      case FIREBIRD:
        cfg.setProperty(Environment.DIALECT, org.hibernate.dialect.FirebirdDialect.class.getName());
        cfg.setProperty(Environment.DRIVER, org.firebirdsql.jdbc.FBDriver.class.getName());
        cfg.setProperty(Environment.URL, "jdbc:firebirdsql://" + configDB.getHost() + "/" + configDB.getPort() + ":" + configDB.getDatabase());
        break;
      case POSTGRES:
        cfg.setProperty(Environment.DIALECT, org.hibernate.dialect.PostgreSQL9Dialect.class.getName());
        cfg.setProperty(Environment.DRIVER, org.postgresql.Driver.class.getName());
        cfg.setProperty(Environment.URL, "jdbc:postgresql://" + configDB.getHost() + ":" + configDB.getPort() + "/" + configDB.getDatabase());
        cfg.setProperty(Environment.DEFAULT_SCHEMA, "public"); //PostgreSQL tem Schema
        break;
      default:
        break;
    }

    cfg.setProperty(Environment.USER, configDB.getUser());
    cfg.setProperty(Environment.PASS, configDB.getPassword());
    cfg.setProperty(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
    cfg.setProperty(Environment.ORDER_UPDATES, "false"); //Força ordenação de updates pela PK, menos Deadlocks.
    cfg.setProperty(Environment.ORDER_INSERTS, "false"); //Força ordenação de inserts pela PK, menos Deadlocks.
    cfg.setProperty(Environment.GENERATE_STATISTICS, "false"); //Coleta estatisticas para otimização.
    cfg.setProperty(Environment.AUTOCOMMIT, "false");
    //cfg.setProperty(Environment.ISOLATION, java.sql.Connection.TRANSACTION_NONE);
    ///cfg.setProperty(Environment.TRANSACTION_COORDINATOR_STRATEGY, JdbcTransactionFactory.class.getName());

    //cfg.setProperty(Environment.HBM2DDL_AUTO, "create-drop"); //Exclui e Recria tudo
    cfg.setProperty(Environment.HBM2DDL_AUTO, "create"); //Criar
    //cfg.setProperty(Environment.HBM2DDL_AUTO, "validate"); //Valida DDL
    //cfg.setProperty(Environment.HBM2DDL_AUTO, "update"); //Atualiza DDL

    cfg.setProperty(Environment.SHOW_SQL, "false"); //Mostra o SQL
    cfg.setProperty(Environment.USE_SQL_COMMENTS, "false"); //Comentários no SQL
    cfg.setProperty(Environment.FORMAT_SQL, "false");

    //Config c3P0
    cfg.setProperty(Environment.C3P0_MIN_SIZE, "10");
    cfg.setProperty(Environment.C3P0_MAX_SIZE, "50");
    cfg.setProperty(Environment.C3P0_TIMEOUT, "300"); //5 min.
    cfg.setProperty(Environment.C3P0_ACQUIRE_INCREMENT, "1");
    cfg.setProperty(Environment.C3P0_MAX_STATEMENTS, "50");
    cfg.setProperty(Environment.CONNECTION_PROVIDER, org.hibernate.c3p0.internal.C3P0ConnectionProvider.class.getName());

    //Config Second Level cache
    cfg.setProperty(Environment.USE_SECOND_LEVEL_CACHE, Boolean.toString(false));
    cfg.setProperty(Environment.USE_QUERY_CACHE, Boolean.toString(false));
    //Cache Com Multiplas Instancias
    //cfg.setProperty(Environment.CACHE_REGION_FACTORY, org.hibernate.cache.ehcache.EhCacheRegionFactory.class.getName());
    //Cache Singleton
    cfg.setProperty(Environment.CACHE_REGION_FACTORY, org.hibernate.cache.ehcache.SingletonEhCacheRegionFactory.class.getName());
    //cfg.setProperty("net.sf.ehcache.configurationResourceName", "_ehcache.xml");

    //Muti-Tenancy
    //DATABASE //Cada Tenant em uma Base de Dados.
    //DISCRIMINATOR //Tudo em um unico BD, utiliza chaves extrangeiras. (A partir da versao 5)
    //SCHEMA //Tudo em um unico BD, utiliza schemas diferentes.
    //NONE
    cfg.setProperty(Environment.MULTI_TENANT, org.hibernate.MultiTenancyStrategy.NONE.toString());
    //cfg.setProperty(Environment.MULTI_TENANT_CONNECTION_PROVIDER, MultiTenantConnectionProviderImp.class.getName());
    //cfg.setProperty(Environment.MULTI_TENANT_IDENTIFIER_RESOLVER, HibernateSchemaResolver.class.getName());

    //cfg.addAnnotatedClass(Preferences.class);
    ServiceRegistry registry = new StandardServiceRegistryBuilder().applySettings(cfg.getProperties()).build();
    sessionFactory = cfg.buildSessionFactory(registry);

    return sessionFactory;
  }

}
