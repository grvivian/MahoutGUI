package br.com.grvivian.hibernate;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.internal.SessionImpl;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.query.NativeQuery;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author glaucio
 */
public class HibernateDataModel extends AbstractDataModel {

  private static final Logger log = LoggerFactory.getLogger(HibernateDataModel.class);

  private final String host;
  private final int port;
  private final String user;
  private final String password;
  private final String database;
  private final String table;
  private final String user_id;
  private final String item_id;
  private final String pref;

  private DataModel delegate;

  public HibernateDataModel(String host, int port, String user, String password, String database, String table, String user_id, String item_id, String pref) {
    this.host = host;
    Preconditions.checkArgument(host.length() > 0L, "Database_host invalid");

    this.port = port;
    Preconditions.checkArgument(port > 0L, "Database_host invalid");

    this.user = user;
    Preconditions.checkArgument(user.length() > 0L, "User invalid");

    this.password = password;
    Preconditions.checkArgument(password.length() > 0L, "Password invalid");

    this.database = database;
    Preconditions.checkArgument(database.length() > 0L, "Database Name invalid");

    this.table = table;
    Preconditions.checkArgument(table.length() > 0L, "Table invalid");

    this.user_id = user_id;
    Preconditions.checkArgument(user_id.length() > 0L, "User ID column invalid");

    this.item_id = item_id;
    Preconditions.checkArgument(item_id.length() > 0L, "Item ID column invalid");

    this.pref = pref;
    Preconditions.checkArgument(pref.length() > 0L, "Preference column invalid");

    log.info("Creating HibernateDataModel");

    try {
      reload();
    } catch (Exception ex) {
      log.warn("Exception in reload construtor", ex);
    }
  }

  protected void reload() throws Exception {
    try {
      delegate = buildModel();
    } catch (IOException ioe) {
      log.warn("Exception while reloading", ioe);
    }
  }

  private static SessionFactory sessionFactory = null;

  protected SessionFactory getSessionFactory(TpBD tpDB) throws Exception {
    if (sessionFactory == null) {
      Configuration cfg = new Configuration();
      if (null != tpDB) {
        switch (tpDB) {
          case MYSQL:
            cfg.setProperty(Environment.DIALECT, org.hibernate.dialect.MySQLInnoDBDialect.class.getName());
            cfg.setProperty(Environment.DRIVER, com.mysql.jdbc.Driver.class.getName());
            cfg.setProperty(Environment.URL, "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database);
            break;
          case FIREBIRD:
            cfg.setProperty(Environment.DIALECT, org.hibernate.dialect.FirebirdDialect.class.getName());
            cfg.setProperty(Environment.DRIVER, org.firebirdsql.jdbc.FBDriver.class.getName());
            cfg.setProperty(Environment.URL, "jdbc:firebirdsql://" + this.host + "/" + this.port + ":" + this.database);
            break;
          case POSTGRES:
            cfg.setProperty(Environment.DIALECT, org.hibernate.dialect.PostgreSQL9Dialect.class.getName());
            cfg.setProperty(Environment.DRIVER, org.postgresql.Driver.class.getName());
            cfg.setProperty(Environment.URL, "jdbc:postgresql://" + this.host + ":" + this.port + "/" + this.database);
            cfg.setProperty(Environment.DEFAULT_SCHEMA, "public"); //PostgreSQL tem Schema
            break;
          default:
            break;
        }
      }
      cfg.setProperty(Environment.USER, this.user);
      cfg.setProperty(Environment.PASS, this.password);
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

      cfg.addAnnotatedClass(Preferences.class);

      ServiceRegistry registry = new StandardServiceRegistryBuilder().applySettings(cfg.getProperties()).build();
      sessionFactory = cfg.buildSessionFactory(registry);
    }

    return sessionFactory;
  }

  private void getTables() {
    Map<String, ClassMetadata> classMetaDataMap = sessionFactory.getAllClassMetadata();

    for (Map.Entry<String, ClassMetadata> metaDataMap : classMetaDataMap.entrySet()) {
      ClassMetadata classMetadata = metaDataMap.getValue();
      AbstractEntityPersister abstractEntityPersister = (AbstractEntityPersister) classMetadata;
      String tableName = abstractEntityPersister.getTableName();
      System.out.println(tableName);
    }

    SessionImpl sessionImpl = (SessionImpl) sessionFactory.openSession();
    try {
      Connection connection = sessionImpl.connection();
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"});

      while (resultSet.next()) {
        String tableName = resultSet.getString(3);
        System.out.println(tableName);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected DataModel buildModel() throws Exception {
    Session s = null;
    Transaction t = null;
    List<Object[]> lista = null;

    try {
      s = getSessionFactory(TpBD.POSTGRES).openSession();
      getTables();
      t = s.beginTransaction();
      //t.begin();
      NativeQuery q = s.createNativeQuery("select "
              + "\"" + user_id + "\" as userId, "
              + "\"" + item_id + "\" as itemId,"
              + pref + " as preference "
              + " from " + table + " as Preferences"
              + " order by \"userId\""); //Important
      lista = q.setCacheable(true).list();
      t.commit();
      //s.getMetamodel().getEntities().
    } catch (HibernateException e) {
      if ((t != null) && (t.isActive())) {
        t.rollback();
      }
      throw e;
    } finally {
      if ((s != null) && (s.isConnected())) {
        s.close();
      }
    }

    FastByIDMap<PreferenceArray> userData = new FastByIDMap<>();
    long lastUserId = 0;
    List<Preference> prefsUser = new ArrayList<>();

    Iterator<Object[]> it = lista.iterator();
    while (it.hasNext()) {
      Object[] p = it.next();

      BigInteger u = (BigInteger) p[0];
      BigInteger i = (BigInteger) p[1];
      float pref = (float) p[2];

      if ((lastUserId == 0) || (u.longValue() == lastUserId)) {
        prefsUser.add(new GenericPreference(u.longValue(), i.longValue(), pref));
      } else {
        userData.put(lastUserId, new GenericUserPreferenceArray(prefsUser));

        prefsUser = new ArrayList<>();
        prefsUser.add(new GenericPreference(u.longValue(), i.longValue(), pref));
      }

      lastUserId = u.longValue();
    }

    return new GenericDataModel(userData);
  }

  @Override
  public LongPrimitiveIterator getUserIDs() throws TasteException {
    return delegate.getUserIDs();
  }

  @Override
  public PreferenceArray getPreferencesFromUser(long l) throws TasteException {
    return delegate.getPreferencesFromUser(l);
  }

  @Override
  public FastIDSet getItemIDsFromUser(long l) throws TasteException {
    return delegate.getItemIDsFromUser(l);
  }

  @Override
  public LongPrimitiveIterator getItemIDs() throws TasteException {
    return delegate.getItemIDs();
  }

  @Override
  public PreferenceArray getPreferencesForItem(long l) throws TasteException {
    return delegate.getPreferencesForItem(l);
  }

  @Override
  public Float getPreferenceValue(long l, long l1) throws TasteException {
    return delegate.getPreferenceValue(l, l1);
  }

  @Override
  public Long getPreferenceTime(long l, long l1) throws TasteException {
    return delegate.getPreferenceTime(l, l1);
  }

  @Override
  public int getNumItems() throws TasteException {
    return delegate.getNumItems();
  }

  @Override
  public int getNumUsers() throws TasteException {
    return delegate.getNumUsers();
  }

  @Override
  public int getNumUsersWithPreferenceFor(long l) throws TasteException {
    return delegate.getNumUsersWithPreferenceFor(l);
  }

  @Override
  public int getNumUsersWithPreferenceFor(long l, long l1) throws TasteException {
    return delegate.getNumUsersWithPreferenceFor(l, l1);
  }

  /**
   * Note that this method only updates the in-memory preference data that this
   * {@link FileDataModel} maintains; it does not modify any data on disk.
   * Therefore any updates from this method are only temporary, and lost when
   * data is reloaded from a file. This method should also be considered
   * relatively slow.
   *
   * @param l User ID
   * @param l1 Item ID
   */
  @Override
  public void setPreference(long l, long l1, float f) throws TasteException {
    delegate.setPreference(l, l1, f);
  }

  @Override
  public void removePreference(long l, long l1) throws TasteException {
    delegate.removePreference(l, l1);
  }

  @Override
  public boolean hasPreferenceValues() {
    return delegate.hasPreferenceValues();
  }

  @Override
  public void refresh(Collection<Refreshable> clctn) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
