/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksdb_admin.thrift.AddS3SstFilesToDBRequest;
import com.pinterest.rocksdb_admin.thrift.Admin;
import com.pinterest.rocksdb_admin.thrift.ClearDBRequest;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.Role;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.bean.ShardBean;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.ShardUtil;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.inject.Inject;

/**
 * This task loads sst files from S3 to a cluster managed by the controller.
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class LoadSSTTask extends TaskBase<LoadSSTTask.Param> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadSSTTask.class);
  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  public LoadSSTTask(String segment,
                     String s3Bucket,
                     String s3Prefix,
                     int concurrency,
                     int rateLimitMbs) {
    this(
        new Param().setSegment(segment)
            .setS3Bucket(s3Bucket)
            .setS3Prefix(s3Prefix)
            .setConcurrency(concurrency)
            .setRateLimitMbs(rateLimitMbs)
    );
  }

  public LoadSSTTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    final String clusterName = ctx.getCluster();
    final String segment = getParameter().getSegment();

    ClusterBean clusterBean = ZKUtil.getClusterConfig(zkClient, clusterName);
    if (clusterBean == null) {
      LOG.error("Failed to get config for cluster {}.", clusterName);
      ctx.getTaskQueue().failTask(ctx.getId(), "Failed to read cluster config from zookeeper.");
      return;
    }
    SegmentBean segmentBean = clusterBean.getSegments()
        .stream()
        .filter(s -> s.getName().equals(segment))
        .findAny()
        .orElse(null);
    if (segmentBean == null) {
      String errMsg = String.format("Segment %s not in cluster %s.", segment, clusterName);
      LOG.error(errMsg);
      ctx.getTaskQueue().failTask(ctx.getId(), errMsg);
      return;
    }

    final ExecutorService executor = Executors.newFixedThreadPool(getParameter().getConcurrency());

    // first pass load sst to masters
    List<Future<Boolean>> futures = new ArrayList<>(segmentBean.getHosts().size());
    for (HostBean host : segmentBean.getHosts()) {
      Future<Boolean> future = executor.submit(() -> loadSSTFromS3(segment, host, Role.MASTER));
      futures.add(future);
    }
    try {
      for (Future<?> future : futures) {
        future.get();
      }
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("First pass failed.", ex);
      ctx.getTaskQueue().failTask(ctx.getId(), "First pass failed. " + ex.getMessage());
      return;
    }

    // second pass load sst to slaves
    futures = new ArrayList<>(segmentBean.getHosts().size());
    for (HostBean host : segmentBean.getHosts()) {
      Future<Boolean> future = executor.submit(() -> loadSSTFromS3(segment, host, Role.SLAVE));
      futures.add(future);
    }
    try {
      for (Future<?> future : futures) {
        future.get();
      }
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("Second pass failed.", ex);
      ctx.getTaskQueue().failTask(ctx.getId(), "Second pass failed. " + ex.getMessage());
      return;
    }

    executor.shutdown();
    executor.shutdownNow();
    ctx.getTaskQueue().finishTask(ctx.getId(), "Finished loading sst to " + clusterName);
  }

  /**
   * Load sst files from s3 to a given host. Only shards that match the given role
   * will have sst files uploaded.
   *
   * @param segmentName segment which sst files belong to
   * @param host        destination host to load sst files
   * @param role        db role
   * @throws TException
   * @return {@code true} if all sst files are successfully loaded.
   */
  private boolean loadSSTFromS3(String segmentName, HostBean host, Role role)
      throws TException {
    Admin.Client client = clientFactory.getClient(host);
    for (ShardBean shard : host.getShards()) {
      if (shard.getRole() == role) {
        String dbName = ShardUtil.getDBNameFromSegmentAndShardId(segmentName, shard.getId());
        String s3Path = ShardUtil.getS3Path(getParameter().getS3Prefix(), shard.getId());
        LOG.info("Clearing db {} on {}...", dbName, host.getIp());
        client.clearDB(new ClearDBRequest(dbName));
        LOG.info("Loading sst from s3://{}/{} to {} on {}...",
            getParameter().getS3Bucket(), s3Path, dbName, host.getIp());
        client.addS3SstFilesToDB(
            new AddS3SstFilesToDBRequest(dbName, getParameter().getS3Bucket(), s3Path)
                .setS3_download_limit_mb(getParameter().getRateLimitMbs())
        );
      }
    }
    LOG.info("Finished loading sst to all {} shards on {}", role.name(), host.getIp());
    return true;
  }

  public static class Param extends Parameter {
    @JsonProperty
    private String segment;

    @JsonProperty
    private String s3Bucket;

    @JsonProperty
    private String s3Prefix;

    @JsonProperty
    private int concurrency;

    @JsonProperty
    private int rateLimitMbs;

    public String getSegment() {
      return segment;
    }

    public Param setSegment(String segment) {
      this.segment = segment;
      return this;
    }

    public String getS3Bucket() {
      return s3Bucket;
    }

    public Param setS3Bucket(String s3Bucket) {
      this.s3Bucket = s3Bucket;
      return this;
    }

    public String getS3Prefix() {
      return s3Prefix;
    }

    public Param setS3Prefix(String s3Prefix) {
      this.s3Prefix = s3Prefix;
      return this;
    }

    public int getConcurrency() {
      return concurrency;
    }

    public Param setConcurrency(int concurrency) {
      this.concurrency = concurrency;
      return this;
    }

    public int getRateLimitMbs() {
      return rateLimitMbs;
    }

    public Param setRateLimitMbs(int rateLimitMbs) {
      this.rateLimitMbs = rateLimitMbs;
      return this;
    }
  }
}
