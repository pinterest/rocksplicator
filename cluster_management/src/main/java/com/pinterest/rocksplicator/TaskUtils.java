package com.pinterest.rocksplicator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * brief: rocksdbsnap
 *
 * we will add Jobs (backup/restore/export segment) and their schedule (one-time or recurrent)
 * through helixrest/UI since we don't do scheduling from TaskDriver in java directly.
 *
 * Helix Task framework will pick up the added jobs (containing tasks) from zk, and distribute
 * tasks to the cluster for execution.
 *
 * (?) spectator monitor job status, and update KV Manager service
 */

/**
 * Utils to initiate tasks, check source status, report task status
 *
 * Backup:
 *  tasks will get the Job scheduling time as the universal curt "version" for segment. It will
 *  check the interval (curt version - last backup version), if over, eg 10min, execute the task;
 *  otherwise skip curt task. One-time job need to specify ts as "version"; recurrent job will
 *  use the recurrent time as "version".
 *  s3 path: /rocksplicator/backup/[cluster]/[segment]/version/[partition]
 *  last backup version: use s3 utils to get the last version with _SUCCESS
 *
 *  (?) spectator will monitor the job status, and update KV manager service
 *
 * Restore:
 *  tasks will get the specified version, and  execution restore from s3
 *
 *  (?) spectator will monitor the restore job status, and update KV manager
 *
 * Export:
 *  tasks will first get the latest un-exported backuped version (or specified version from Task
 *  config) and update zNode: /rocksdbsnap/export/[cluster]/[segmentName]: {version:1234}. The
 *  first Task
 *
 *  (?) spectator monitor job status, and update KV manager
 */


public class TaskUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TaskUtils.class);

  // TODO: move this enum into KV Manager service
  public enum BackupStatus {
    /**
     * The task has yet to start
     */
    NOT_STARTED,
    /**
     * The task is in progress.
     */
    IN_PROGRESS,
    /**
     * The task has been paused. It may be resumed later.
     */
    PAUSED,
    /**
     * The task is in stopping process. Will complete if subtasks are paused or completed
     */
    STOPPING,
    /**
     * The task has failed. It cannot be resumed.
     */
    FAILED,
    /**
     * All the task partitions have completed normally.
     */
    COMPLETED,
  }

  public enum ExportStatus {
    NOT_STARTED,
    IN_PROGRESS,
    PAUSED,
    STOPPING,
    FAILED,
    COMPLETED,
  }

  /**
   * endpoint to get the latest version
   * TODO: move this to KV Manager service
   */
  public static long getLatestResourceVersion(String cluster, String segmentName)
      throws RuntimeException {
    // if not existed, return -1

    String srcStateUrl =
        String
            .format(
                "http://zkservice.pinadmin.com:8082/manageddata/map/rocksplicator/%s_state?",
                cluster);

    HttpGet httpGet = new HttpGet(srcStateUrl);
    HttpClient httpclient = new DefaultHttpClient();

    try {
      HttpResponse response = httpclient.execute(httpGet);
      if (response.getStatusLine().getStatusCode() == 200) {
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          String json = EntityUtils.toString(entity);
          Map<String, Map<String, Map<String, String>>> stateMap = jobStateJsonToMap(json);

          if (stateMap.containsKey(segmentName)) {
            Map<String, Map<String, String>> resVersionStates =
                new TreeMap<>(Collections.reverseOrder());
            resVersionStates.putAll(stateMap.get(segmentName));
            for (String resVersion : resVersionStates.keySet()) {
              Set<String> jobStates = new HashSet<>(resVersionStates.get(resVersion).values());
              if (jobStates.contains("COMPLETED")) {
                return Long.parseLong(resVersion);
              }
            }
          } else {
            System.out.println("state map does not contain versions for segment: " + segmentName);
          }
        } else {
          LOG.error("HttpGet entity is empty");
        }
      } else {
        LOG.error(response.getStatusLine().getReasonPhrase());
      }
    } catch (Exception e) {
      LOG.error("Failed to get the state map, ", e);
      throw new RuntimeException(e);
    }

    return -1;
  }

  /**
   * endpoint to retrieve Backup status from KVManager service
   * ToDO: get status from KV manager service
   */
  public static BackupStatus getBackupStatus(String cluster, String segmentName, long version)
      throws RuntimeException {
    // if not existed, return BackupStatus.NONINIT

    LOG.error(String
        .format("get Backup Status for {cluster: %s, segment: %s, version: %d}", cluster,
            segmentName, version));

    return BackupStatus.COMPLETED;
  }

  /**
   * endpoint to update Backup status
   * ToDO: get status from KV manager service
   */
  public static void updateBackupStatus(String cluster, String segmentName, long version,
                                        BackupStatus status) {

  }

  /**
   * endpoint to retrieve export job status from ConfigV3
   * TODO: get status from KVManager service
   *
   * if for resource version, other job version is in {IN_PROGRESS, COMPLETED}, curt job abandoned
   * return COMPLETED else if for resource version, curt job version is not COMPLETED, then,
   * continue execute
   */
  public static ExportStatus getExportStatus(String cluster, String segmentName,
                                             long resourceVersion, long jobVersion)
      throws RuntimeException {

    LOG.error(String
        .format("get Export Status for {cluster: %s, segment: %s, version: %d}", cluster,
            segmentName, resourceVersion));

    String srcStateUrl =
        String
            .format(
                "http://zkservice.pinadmin.com:8082/manageddata/map/rocksplicator/%s_state?",
                cluster);

    HttpGet httpGet = new HttpGet(srcStateUrl);
    HttpClient httpclient = new DefaultHttpClient();

    try {
      HttpResponse response = httpclient.execute(httpGet);
      if (response.getStatusLine().getStatusCode() == 200) {
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          String json = EntityUtils.toString(entity);
          Map<String, Map<String, Map<String, String>>> stateMap = jobStateJsonToMap(json);

          if (stateMap.containsKey(segmentName)) {
            Map<String, Map<String, String>> resVersionStates = stateMap.get(segmentName);
            String resVersion = String.valueOf(resourceVersion);

            if (resVersionStates.containsKey(resVersion)) {

              // if other job version is running task on the same resource version, skip curt job
              for (String jobV : resVersionStates.get(resVersion).keySet()) {
                if (!jobV.equals(String.valueOf(jobVersion))) {
                  String jobState = resVersionStates.get(resVersion).get(jobV);
                  if (jobState.equals("COMPLETED") || jobState.equals("IN_PROGRESS")) {
                    return ExportStatus.COMPLETED;
                  }
                }
              }

              // return curt job version 's job state
              if (resVersionStates.get(resVersion).containsKey(jobVersion)) {
                String jobState = resVersionStates.get(resVersion).get(jobVersion);

                if (jobState.equals("COMPLETED")) {
                  return ExportStatus.COMPLETED;
                }
                if (jobState.equals("IN_PROGRESS")) {
                  return ExportStatus.IN_PROGRESS;
                }
                if (jobState.equals("PAUSED")) {
                  return ExportStatus.PAUSED;
                }
              }

            } else {
              LOG.error("state map does not contain version " + resVersion + " for segment: "
                  + segmentName);
            }
          } else {
            LOG.error("state map does not contain segment: " + segmentName);
          }
        } else {
          LOG.error("HttpGet entity is empty");
        }
      } else {
        LOG.error(response.getStatusLine().getReasonPhrase());
      }
    } catch (Exception e) {
      LOG.error("Failed to get the state map", e);
      throw new RuntimeException(e);
    }

    return ExportStatus.NOT_STARTED;
  }

  /**
   * endpoint to update export status
   * ToDO: update status from KV manager service
   */
  public static void updateExportStatus(String cluster, String segmentName, long version,
                                        ExportStatus status) {

  }

  /**
   * lock segment by segment and job creation time
   */
  public static String getSegmentLockPath(String taskCluster, String resourceCluster,
                                          String segment, long jobCreationTime) {
    return String
        .format("/dedup-test/rocksplicator/%s/%s/%s/%s/lock", taskCluster, resourceCluster,
            segment,
            String.valueOf(jobCreationTime));
  }

  /**
   * build statePostUrl from configPostUrl
   * e.g. configPostUrl: String.format("http://[prefix]/%s?mode=overwrite", cluster)
   *      statePostUrl: String.format("http://[prefix]/%s_state?mode=overwrite", cluster)
   * if some other statePostUrl wanted, overrite this build function
   */
  public static String buildStatePostUrl(String configPostUrl) {
    int questMarkIndex = configPostUrl.indexOf('?');
    String statePostUrl =
        configPostUrl.substring(0, questMarkIndex) + "_state" + configPostUrl
            .substring(questMarkIndex);
    return statePostUrl;
  }

  public static Map<String, Map<String, Map<String, String>>> jobStateJsonToMap(String json)
      throws Exception {
    ObjectMapper jsonMapper = new ObjectMapper();
    TypeReference<Map<String, Map<String, Map<String, String>>>> typeRef =
        new TypeReference<Map<String, Map<String, Map<String, String>>>>() {};
    Map<String, Map<String, Map<String, String>>>
        stateMap =
        jsonMapper.readValue(json, typeRef);
    return stateMap;
  }

}
