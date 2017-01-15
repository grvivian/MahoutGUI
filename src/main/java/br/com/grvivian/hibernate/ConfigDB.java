package br.com.grvivian.hibernate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

/**
 * @author glaucio
 */
public class ConfigDB {

  private static final String CONFIG_FILE = "./configMahoutGUI.properties";
  private TpDB tpDB = TpDB.POSTGRES;
  private String host = "127.0.0.1";
  private int port = 5432;
  private String user = "postgres";
  private String password = "1234";
  private String database = "movielens";
  private String table = "movies";
  private String user_id = "userId";
  private String item_id = "movieId";
  private String pref = "rating";

  public void writeConfig() throws Exception {
    Properties p = new Properties();

    p.setProperty("tpDB", tpDB.name());
    p.setProperty("host", host);
    p.setProperty("port", String.valueOf(port));
    p.setProperty("user", user);
    p.setProperty("password", password);
    p.setProperty("database", database);
    p.setProperty("table", table);
    p.setProperty("user_id", user_id);
    p.setProperty("item_id", item_id);
    p.setProperty("pref", pref);

    FileOutputStream arq = new FileOutputStream(CONFIG_FILE);
    p.storeToXML(arq, "", "UTF-8");
    arq.close();
  }

  public void readConfig() throws Exception {
    File f = new File(CONFIG_FILE);

    if (!f.exists()) {
      return;
    }

    Properties p = new Properties();
    FileInputStream arq = new FileInputStream(f);
    p.loadFromXML(arq);
    arq.close();

    tpDB = Enum.valueOf(TpDB.class, p.getProperty("tpDB"));
    host = p.getProperty("host");
    port = Integer.parseInt(p.getProperty("port"));
    user = p.getProperty("user");
    password = p.getProperty("password");
    database = p.getProperty("database");
    table = p.getProperty("table");
    user_id = p.getProperty("user_id");
    item_id = p.getProperty("item_id");
    pref = p.getProperty("pref");
  }

  public TpDB getTpDB() {
    return tpDB;
  }

  public void setTpDB(TpDB tpDB) {
    this.tpDB = tpDB;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getUser_id() {
    return user_id;
  }

  public void setUser_id(String user_id) {
    this.user_id = user_id;
  }

  public String getItem_id() {
    return item_id;
  }

  public void setItem_id(String item_id) {
    this.item_id = item_id;
  }

  public String getPref() {
    return pref;
  }

  public void setPref(String pref) {
    this.pref = pref;
  }

}
