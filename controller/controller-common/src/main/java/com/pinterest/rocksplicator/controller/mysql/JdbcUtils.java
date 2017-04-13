package com.pinterest.rocksplicator.controller.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Util functions for JDBC operations.
 */
public class JdbcUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);
  private static final int MAX_MYSQL_EXECUTE_RETRY = 3;
  private static int QUERYING_INTERVAL_IN_MILLISECONDS = 3000;
  private static final String DB_URL_TEMPLATE = "jdbc:mysql://%s:%d/%s";

  public static String constructJdbcUrl(String dbHost, int dbPort, String dbName) {
    return String.format(DB_URL_TEMPLATE, dbHost, dbPort, dbName);
  }

  /**
   * Create a mysql connection.
   */
  public static Connection createMySqlConnection(String dbURL) {
    try {
      Class.forName("com.mysql.jdbc.Driver");
      Connection mysqlConnection = DriverManager.getConnection(dbURL, "root", "");
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
        LOG.debug("Could not close JDBC Connection", e);
      } catch (Throwable e) {
        LOG.info("Unexpected exception on closing JDBC Connection", e);
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
        LOG.debug("Could not close JDBC Statement", e);
      } catch (Throwable e) {
        LOG.info("Unexpected exception on closing JDBC Statement", e);
      }
    }
  }

  /**
   * Execute mysql update query
   * @param dbURL: db url
   * @param updateSql: update query to execute
   * @return
   */
  public static boolean executeUpdateQuery(String dbURL, String updateSql) {
    for (int i = 0; i < MAX_MYSQL_EXECUTE_RETRY; i ++) {
      Connection connection = createMySqlConnection(dbURL);
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
        closeConnection(connection);
        // TODO(shu): exponential backoff?
        try {
          Thread.sleep(QUERYING_INTERVAL_IN_MILLISECONDS);
        } catch (InterruptedException e) {}
      }
    }
    return false;
  }

}
