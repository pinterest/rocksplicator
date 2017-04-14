package com.pinterest.rocksplicator.controller.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Util functions for JDBC operations.
 */
public class JdbcUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);
  private static final int MAX_MYSQL_EXECUTE_RETRY = 3;
  private static int QUERYING_INTERVAL_IN_MILLISECONDS = 3000;
  private static final String DB_URL_TEMPLATE = "jdbc:mysql://%s:%d/%s";

  /**
   * Create a mysql connection.
   */
  public static Connection createMySqlConnection(
      String dbHost, int dbPort, String dbName, String userName, String password) {
    String dbURL = String.format(DB_URL_TEMPLATE, dbHost, dbPort, dbName);
    try {
      Class.forName("com.mysql.jdbc.Driver");
      Connection mysqlConnection = DriverManager.getConnection(dbURL, userName, password);
      mysqlConnection.setAutoCommit(false);
      return mysqlConnection;
    } catch (ClassNotFoundException e) {
      LOG.error("Cannot initialize jdbc driver class", e);
      return null;
    } catch (SQLException e) {
      LOG.error("Cannot establish connection to MySQL: " + dbURL, e);
      return null;
    }
  }

  /**
   * close the connection.
   *
   * @param connection the connection to be closed.
   */
  public static void closeConnection(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        LOG.error("Could not close JDBC Connection", e);
      } catch (Throwable e) {
        LOG.error("Unexpected exception on closing JDBC Connection", e);
      }
    }
  }

  /**
   * close the statement.
   *
   * @param statement the statement to be closed.
   */
  public static void closeStatement(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        LOG.error("Could not close JDBC Statement", e);
      } catch (Throwable e) {
        LOG.error("Unexpected exception on closing JDBC Statement", e);
      }
    }
  }

  /**
   * Execute mysql update query
   * @param connection: mysql db connection
   * @param updateSql: update query to execute
   * @return: true if update executed successfully, false if else.
   */
  public static boolean executeUpdateQuery(Connection connection, String updateSql) {
    for (int i = 0; i < MAX_MYSQL_EXECUTE_RETRY; i ++) {
      if (connection == null) {
        return false;
      }
      Statement statement = null;
      try {
        statement = connection.createStatement();
        statement.executeUpdate(updateSql);
        connection.commit();
        return true;
      } catch (SQLException e) {
        LOG.error(String.format("Query %s execution failed and this is %d try", updateSql, i+1), e);
      } finally {
        closeStatement(statement);
        // TODO(shu): exponential backoff?
        try {
          Thread.sleep(QUERYING_INTERVAL_IN_MILLISECONDS);
        } catch (InterruptedException e) {}
      }
    }
    return false;
  }


  /**
   * Execute mysql query.
   * @param connection: mysql db connection
   * @param querySql: query to execute
   * @return a list of query result, null if else.
   */
  public static List<HashMap<String, Object>> executeQuery(Connection connection, String querySql) {
    for (int i = 0; i < MAX_MYSQL_EXECUTE_RETRY; i ++) {
      Statement statement = null;
      ResultSet resultSet = null;
      try {
        statement = connection.createStatement();
        resultSet = statement.executeQuery(querySql);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<HashMap<String, Object>> results = new ArrayList<>();
        while (resultSet.next()) {
          HashMap<String, Object> result = new HashMap<>();
          for (int j = 1; j <= resultSetMetaData.getColumnCount(); j ++) {
            String columnLabel = resultSetMetaData.getColumnLabel(j);
            int columnType = resultSetMetaData.getColumnType(j);
            switch (columnType) {
              case Types.TINYINT:
                result.put(columnLabel, resultSet.getInt(j));
                break;
              case Types.BIGINT:
                result.put(columnLabel, resultSet.getLong(j));
                break;
              default:
                result.put(columnLabel, resultSet.getString(j));
                break;
            }
          }
          results.add(result);
        }
        return results;
      } catch (SQLException e) {
        LOG.error(String.format("Query %s execution failed and this is %d try", querySql, i+1), e);
      } finally {
        closeStatement(statement);
        // TODO(shu): exponential backoff?
        try {
          Thread.sleep(QUERYING_INTERVAL_IN_MILLISECONDS);
        } catch (InterruptedException e) {}
      }
    }
    return null;
  }
}
