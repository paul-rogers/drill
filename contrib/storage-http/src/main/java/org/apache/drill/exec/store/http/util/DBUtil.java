/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.http.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;

public class DBUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DBUtil.class);

  private static final Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();

  private static String table = "student";

  private static String allDataDB = "student";

  private static List<String> part_data_DBs = new ArrayList<String>();

  //private static int sub_students_count = 8;
  private static int batch_insert_count = 10000;

  private static boolean notInsertTestData = true;


  public static List<String> getPart_data_DBs() {
    return part_data_DBs;
  }


  public static void setPart_data_DBs(List<String> part_data_DBs) {
    DBUtil.part_data_DBs = part_data_DBs;
  }



/*  private static String student_new = "student_new";
  private static String student_all = "student_all";*/


  public static DataSource createDataSource(String driver, String url, String userName, String password) {

    BasicDataSource source = new BasicDataSource();
    source.setDriverClassName(driver);
    source.setUrl(url);

    if (userName != null) {
      source.setUsername(userName);
    }

    if (password != null) {
      source.setPassword(password);
    }

    source.setPoolPreparedStatements(true);
    source.setInitialSize(1);
    try {
      // initial a connection
      Connection conn = source.getConnection();
      conn.close();
    } catch (SQLException sqlE) {

      logger.error("db connection error: ", sqlE);
    }
    return source;

  }


  public static void createDataSourceCache(HttpStoragePluginConfig httpConfig) {

    Map<String, Map<String, String>> dbConfig = httpConfig.getDbConfig();
    initDataSource(dbConfig);

    Map<String, String> config = httpConfig.getConfig();
    if (config != null && "true".equals(config.get("insertTestData")) && notInsertTestData) {
      try {
        notInsertTestData = false;
        insertTestData();
      } catch (Exception e) {
        logger.error("insert test data error: ", e);
      }
    }

/*		dataSourceMap.put("mayun", createDataSource("com.mysql.jdbc.Driver", "jdbc:mysql://mayun:3306/drill", "root", "mayun"));
		
		dataSourceMap.put(table, createDataSource("com.mysql.jdbc.Driver", "jdbc:mysql://mayun:3306/"+ table, "root", "mayun"));
		for (int i = 0; i < sub_students_count; i++) {
			DataSource dataSource = createDataSource("com.mysql.jdbc.Driver", "jdbc:mysql://mayun:3306/"+ part_data_DBs.get(i), "root", "mayun");
			dataSourceMap.put(part_data_DBs.get(i), dataSource);
		}*/


    logger.info("dataSourceMap: " + dataSourceMap);


  }


  private static synchronized void initDataSource(Map<String, Map<String, String>> dbConfig) {

    if (dataSourceMap.size() > 0) {
      return;
    }
    logger.info("start to initial data sources");
    for (String dbName : dbConfig.keySet()) {
      Map<String, String> db = dbConfig.get(dbName);

      if (!allDataDB.equals(dbName)) {
        part_data_DBs.add(dbName);
      }

      DataSource dataSource = createDataSource(db.get("driver"), db.get("url"), db.get("username"), db.get("password"));
      dataSourceMap.put(dbName, dataSource);
    }

    logger.info("part_data_DBs: " + part_data_DBs.toString());
  }

  public static DataSource getDataSourceCache(String hostName) {

    return dataSourceMap.get(hostName);


  }

  public static void main(String[] args) throws Exception {
		
/*		Set<String> testSet=new HashSet<String>();
			  for(int i =1;i<=sub_students_count;i++){
				  testSet.add(table+i);
				  part_data_DBs.add(table+i);
			  }*/


    createDataSourceCache(null);
    //deleteTestDataTable();
    //createTestDataTable();
    insertTestData();
  }

  public static synchronized void insertTestData() throws Exception {
    //createDataSourceCache(null);
    //ResultSet resultSet;

    logger.debug("start to insert test data");
    Connection conn = null;
    PreparedStatement pst = null;

    Connection connForAllData = dataSourceMap.get(allDataDB).getConnection();
    PreparedStatement pstForAllData = connForAllData.prepareStatement("");

    for (int i = 0; i < part_data_DBs.size(); i++) {

      conn = dataSourceMap.get(part_data_DBs.get(i)).getConnection();
      pst = conn.prepareStatement("");
      int fromId = (i + 1) * 10000000 + 1;
      int toId = fromId + 2999999;

      //if ("student1".equals(part_data_DBs.get(i)) || "student2".equals(part_data_DBs.get(i))) {
      insertTestData(pst, table, fromId, toId, conn);
      //}
      insertTestData(pstForAllData, table, fromId, toId, connForAllData);


    }

    connForAllData.close();
    pstForAllData.close();
    if (pst != null) {
      pst.close();
    }
    if (conn != null) {
      conn.close();
    }

    // insert to student
/*			connection = dataSourceMap.get(all_data_table).getConnection();
		    statement = connection.createStatement();
			insertTestData(statement, all_data_table, 10000001, 90000000);*/

/*		// delete data before insert
		// insert to student
		statement.execute("delete from " + student);
		insertTestata(statement, student, 1000001, 2000000);
		// delete data before insert
		// insert to student_new
		statement.execute("delete from " + student_new);
		insertTestata(statement, student_new, 2000001, 3000000);
		// delete data before insert
		// insert to student_all
		statement.execute("delete from " + student_all);
		insertTestata(statement, student_all, 1000001, 3000000);

		// insert to student
		insertTestData(statement, student, 10000001, 20000000);

		// insert to student_new
		insertTestData(statement, student_new, 20000001, 30000000);

		// insert to student_all
		insertTestData(statement, student_all, 10000001, 30000000);
		
		*/

		  
/*		// insert to student_new
		insertTestData(statement, student2, 20000001, 30000000);

		// insert to student_all
		insertTestData(statement, student_all, 10000001, 30000000);*/


  }


  private static void insertTestData(Statement statement, String table, int keyFrom, int keyTo, Connection conn) throws SQLException {

    String initSql = "insert into " + table + "(id, name, sex, code, score) values";
    StringBuffer insertSqlSb = new StringBuffer(initSql);

    // int batchCount=100;
    for (int i = keyFrom; i <= keyTo; i++) {

      insertSqlSb.append("(");
      insertSqlSb.append(i + ", ");
      insertSqlSb.append("'Tom" + (i % 100) + "', ");
      insertSqlSb.append((i % 2) + ", ");
      insertSqlSb.append(i % 1000 + ", ");
      insertSqlSb.append(i * 10000);
      insertSqlSb.append(")");

      if (i % batch_insert_count == 0) {
        statement.addBatch(insertSqlSb.toString());
        statement.executeBatch();
        //conn.commit();
        insertSqlSb = new StringBuffer(initSql);
      } else {

        insertSqlSb.append(",");
      }

    }

  }


  private static void createTestDataTable() throws SQLException {

    // ResultSet resultSet;
    Connection connection;
    Statement statement;

    connection = dataSourceMap.get(table).getConnection();
    statement = connection.createStatement();
    statement.execute("create table " + table + "(id bigint not null primary key , name char(20) not null, sex char(2) not null default '0', code int, score bigint)");

    for (int i = 0; i < part_data_DBs.size(); i++) {

      connection = dataSourceMap.get(part_data_DBs.get(i)).getConnection();
      statement = connection.createStatement();

      statement.execute("create table " + table + "(id bigint not null primary key , name char(20) not null, sex char(2) not null default '0', code int, score bigint)");

    }
    statement.close();
    connection.close();
  }


  private static void deleteTestDataTable() throws SQLException {

    createDataSourceCache(null);
    // ResultSet resultSet;
    Connection connection;
    Statement statement;


    // delete data before insert
    // insert to student
    connection = dataSourceMap.get(table).getConnection();
    statement = connection.createStatement();
    statement.execute("delete from " + table);

    for (int i = 0; i < part_data_DBs.size(); i++) {

      connection = dataSourceMap.get(part_data_DBs.get(i)).getConnection();
      statement = connection.createStatement();
      statement.execute("delete from " + table);
    }
/*
		// delete data before insert
		// insert to student_new
		statement.execute("delete from " + student_new);

		// delete data before insert
		// insert to student_all
		statement.execute("delete from " + student_all);*/


    statement.close();
    connection.close();
  }


}
