/// Copyright 2017 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author bol (bol@pinterest.com)
//

package com.pinterest.rocksplicator;

import com.pinterest.rocksdb_admin.thrift.AddDBRequest;
import com.pinterest.rocksdb_admin.thrift.Admin;
import com.pinterest.rocksdb_admin.thrift.AdminException;
import com.pinterest.rocksdb_admin.thrift.ClearDBRequest;
import com.pinterest.rocksdb_admin.thrift.CloseDBRequest;

import org.apache.helix.model.Message;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Build a thrift client to local adminPort
   * @param adminPort
   * @return a client object
   */
  public static Admin.Client getLocalAdminClient(int adminPort) throws TTransportException {
    TSocket sock = new TSocket("localhost", adminPort);
    sock.open();
    return new Admin.Client(new TBinaryProtocol(sock));
  }

  /**
   * Convert a partition name into DB name.
   * @param partitionName  e.g. "p2p1_1"
   * @return e.g. "p2p100001"
   */
  public static String getDbName(String partitionName) {
    String[] parts = partitionName.split("_");
    return String.format("%s%05d", parts[0], Integer.parseInt(parts[1]));
  }

  /**
   * Clear the content of a DB and leave it as closed.
   * @param dbName
   * @param adminPort
   */
  public static void clearDB(String dbName, int adminPort) {
    try {
      Admin.Client client = getLocalAdminClient(adminPort);
      ClearDBRequest req = new ClearDBRequest(dbName);
      req.setReopen_db(false);
      client.clearDB(req);
    } catch (AdminException e) {
      LOG.error("Failed to destroy DB", e);
    } catch (TTransportException e) {
      LOG.error("Failed to connect to local Admin port", e);
    } catch (TException e) {
      LOG.error("ClearDB() request failed", e);
    }
  }

  /**
   * Close a DB
   * @param dbName
   * @param adminPort
   */
  public static void closeDB(String dbName, int adminPort) {
    try {
      Admin.Client client = getLocalAdminClient(adminPort);
      CloseDBRequest req = new CloseDBRequest(dbName);
      client.closeDB(req);
    } catch (AdminException e) {
      LOG.info(dbName + " doesn't exist", e);
    } catch (TTransportException e) {
      LOG.error("Failed to connect to local Admin port", e);
    } catch (TException e) {
      LOG.error("CloseDB() request failed", e);
    }
  }

  /**
   * Add a DB as a Slave, and set it upstream to be itself
   * @param dbName
   * @param adminPort
   */
  public static void addDB(String dbName, int adminPort) {
    try {
      Admin.Client client = getLocalAdminClient(adminPort);
      AddDBRequest req = new AddDBRequest(dbName, "127.0.0.1");
      client.addDB(req);
    } catch (AdminException e) {
      LOG.info("Failed to open " + dbName, e);
    } catch (TTransportException e) {
      LOG.error("Failed to connect to local Admin port", e);
    } catch (TException e) {
      LOG.error("AddDB() request failed", e);
    }
  }

  /**
   * Log transition meesage
   * @param message
   */
  public static void logTransitionMessage(Message message) {
    LOG.info("Switching from " + message.getFromState() + " to " + message.getToState()
        + " for " + message.getPartitionName());
  }
}
