package com.pinterest.rocksplicator;

import static com.pinterest.rocksplicator.Utils.LOCAL_HOST_IP;

import com.pinterest.cdc_admin.thrift.CdcAdmin;
import com.pinterest.cdc_admin.thrift.CDCAdminException;
import com.pinterest.cdc_admin.thrift.AddObserverRequest;
import com.pinterest.cdc_admin.thrift.AddObserverResponse;
import com.pinterest.cdc_admin.thrift.RemoveObserverRequest;
import com.pinterest.cdc_admin.thrift.RemoveObserverResponse;
import com.pinterest.rocksplicator.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;


public class CdcUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CdcUtils.class);

  /**
   * Build a thrift client to local adminPort
   * @param adminPort
   * @return a CDC Admin client object
   * @throws TTransportException
   */
  public static CdcAdmin.Client getLocalCdcAdminClient(int adminPort) throws TTransportException {
    return getCdcAdminClient(Utils.LOCAL_HOST_IP, adminPort);
  }

  /**
   * Build a thrift client to host:adminPort
   * @param host
   * @param adminPort
   * @return a client object
   * @throws TTransportException
   */
  public static CdcAdmin.Client getCdcAdminClient(String host, int adminPort)
      throws TTransportException {
    TSocket sock = new TSocket(host, adminPort);
    sock.open();
    return new CdcAdmin.Client(new TBinaryProtocol(sock));
  }


  /**
   * Add an observer for a db
   * @param dbName The name of the partition to observe
   * @param upstreamAddr The upstream address of the leader for the partition
   * @param adminPort the port to make the request on
   */
  public static AddObserverResponse addObserver(String dbName, String upstreamAddr, int adminPort) {
    try {
      LOG.error("Add observer for: " + dbName);
      CdcAdmin.Client client = getLocalCdcAdminClient(adminPort);
      AddObserverRequest req = new AddObserverRequest(dbName, upstreamAddr);
      return client.addObserver(req);
    } catch (CDCAdminException e) {
      LOG.error("Cdc admin exception when adding observer for " + dbName);
      throw new RuntimeException(e);
    } catch (TTransportException e) {
      LOG.error("Failed to connect to Admin port", e);
      throw new RuntimeException(e);
    } catch (TException e) {
      LOG.error("addObserver() request failed", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Remove an observer
   * @param dbName the name of the db to remove the observer of
   * @param adminPort the port to make the request on
   */
  public static RemoveObserverResponse removeObserver(String dbName, int adminPort) {
    try {
      LOG.error("Remove observer for: " + dbName);
      CdcAdmin.Client client = getLocalCdcAdminClient(adminPort);
      RemoveObserverRequest req = new RemoveObserverRequest(dbName);
      return client.removeObserver(req);
    } catch (CDCAdminException e) {
      LOG.error(dbName + " doesn't exist", e);
      throw new RuntimeException(e);
    } catch (TTransportException e) {
      LOG.error("Failed to connect to Admin port", e);
      throw new RuntimeException(e);
    } catch (TException e) {
      LOG.error("removeObserver() request failed", e);
      throw new RuntimeException(e);
    }
  }


}
