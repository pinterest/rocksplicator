package com.pinterest.rocksplicator.shardmap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class JsonShardMap implements ShardMap {

  private final Map<String, ResourceMap> resourceMap;

  JsonShardMap(JSONObject shardMapObj) {
    resourceMap = new HashMap<>(shardMapObj.size());

    for (Object resourceNameObj : shardMapObj.keySet()) {
      String resourceName = (String) resourceNameObj;
      JSONObject resourceObj = (JSONObject) shardMapObj.get(resourceNameObj);
      if (resourceObj != null) {
        resourceMap.put(resourceName, new JsonResourceMapImpl(resourceName, resourceObj));
      }
    }
  }

  @Override
  public Set<String> getResources() {
    return resourceMap.keySet();
  }

  @Override
  public ResourceMap getResourceMap(String resource) {
    return resourceMap.get(resource);
  }


  private class JsonResourceMapImpl implements ResourceMap {

    private final String resourceName;
    private final int numShards;

    private Map<Partition, List<Replica>> replicasByPartition;
    private Map<Instance, List<Replica>> replicasByInstance;
    private Set<Partition> allPartitions;

    JsonResourceMapImpl(String resourceName, JSONObject resourceMapObj) {
      this.resourceName = resourceName;
      this.numShards = (int) resourceMapObj.get("num_shards");

      ImmutableSet.Builder<Partition> setBuilder = ImmutableSet.builder();
      for (int partitionId = 0; partitionId < numShards; ++partitionId) {
        setBuilder.add(new Partition(resourceName, partitionId));
      }
      this.allPartitions = setBuilder.build();

      // Now for all instance info, construct Replicas information.
      for (Object key : resourceMapObj.keySet()) {
        if ("num_shards".equalsIgnoreCase((String) key)) {
          continue;
        }
        String encodedInstanceStr = (String) key;
        Instance instance = new Instance(encodedInstanceStr);
        JSONArray partitionsOnInstance = (JSONArray) resourceMapObj.get(key);
        for (Object encodedPartitionObj : partitionsOnInstance) {
          String encodedPartitionStr = (String) encodedPartitionObj;
          String parts[] = encodedInstanceStr.split(":");
          Partition partition = new Partition(parts[0]);
          ReplicaState replicaState = ReplicaState.ONLINE;
          if (parts.length == 2) {
            String state = parts[1];
            if (":M".equalsIgnoreCase(state)) {
              replicaState = ReplicaState.LEADER;
            } else {
              replicaState = ReplicaState.FOLLOWER;
            }
          }

          Replica replica = new Replica(partition, instance, replicaState);
          if (!replicasByPartition.containsKey(partition)) {
            replicasByPartition.put(partition, new ArrayList<>());
          }
          replicasByPartition.get(partition).add(replica);

          if (!replicasByInstance.containsKey(instance)) {
            replicasByInstance.put(instance, new LinkedList<>());
          }
          replicasByInstance.get(instance).add(replica);
        }
      }
    }

    @Override
    public String getResource() {
      return resourceName;
    }

    @Override
    public int getNumShards() {
      return numShards;
    }

    @Override
    public Set<Instance> getInstances() {
      return replicasByInstance.keySet();
    }

    @Override
    public Set<Partition> getAllKnownPartitions() {
      return replicasByPartition.keySet();
    }

    @Override
    public Set<Partition> getAllMissingPartitions() {
      return Sets.difference(allPartitions, replicasByPartition.keySet());
    }

    @Override
    public Set<Partition> getAllPartitions() {
      return allPartitions;
    }

    @Override
    public List<Replica> getAllReplicasForPartition(Partition partition) {
      if (replicasByPartition.containsKey(partition)) {
        return replicasByPartition.get(partition);
      } else {
        return ImmutableList.of();
      }
    }

    @Override
    public List<Replica> getAllReplicasOnInstance(Instance instance) {
      if (replicasByInstance.containsKey(instance)) {
        return replicasByInstance.get(instance);
      }
      return ImmutableList.of();
    }
  }
}
