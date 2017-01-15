package br.com.grvivian.hibernate;

import com.google.common.base.Preconditions;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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
import org.hibernate.Transaction;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author glaucio
 */
public class HibernateDataModel extends AbstractDataModel {

  private ConfigDB configDB;
  private static final Logger log = LoggerFactory.getLogger(HibernateDataModel.class);

  private DataModel delegate;

  public HibernateDataModel(ConfigDB configDB) {
    this.configDB = configDB;

    Preconditions.checkArgument(configDB.getHost().length() > 0L, "Database_host invalid");
    Preconditions.checkArgument(configDB.getPort() > 0L, "Database_host invalid");
    Preconditions.checkArgument(configDB.getUser().length() > 0L, "User invalid");
    Preconditions.checkArgument(configDB.getPassword().length() > 0L, "Password invalid");
    Preconditions.checkArgument(configDB.getDatabase().length() > 0L, "Database Name invalid");
    Preconditions.checkArgument(configDB.getTable().length() > 0L, "Table invalid");
    Preconditions.checkArgument(configDB.getUser_id().length() > 0L, "User ID column invalid");
    Preconditions.checkArgument(configDB.getItem_id().length() > 0L, "Item ID column invalid");
    Preconditions.checkArgument(configDB.getPref().length() > 0L, "Preference column invalid");

    log.info("Creating HibernateDataModel");

    try {
      reload();
    } catch (Exception ex) {
      log.warn("Exception in reload construtor", ex);
    }
  }

  protected void reload() {
    try {
      delegate = buildModel();
    } catch (Exception ioe) {
      log.warn("Exception while reloading", ioe);
    }
  }

  protected DataModel buildModel() throws Exception {
    Session s = null;
    Transaction t = null;
    List<Object[]> lista = null;

    try {
      s = HibernateUtil.getSessionFactory(this.configDB).openSession();
      t = s.beginTransaction();
      //t.begin();
      NativeQuery q = s.createNativeQuery("select "
              + this.configDB.getUser_id() + " , "//as userId
              + this.configDB.getItem_id() + " , "//as itemId
              + this.configDB.getPref() + "  "//as preference
              + " from " + this.configDB.getTable() + "  "//as Preferences
              + " order by 1"); //Important
      lista = q.setCacheable(true).list();
      t.commit();
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
   * @param f Evaluation value
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
    reload();
  }

}
