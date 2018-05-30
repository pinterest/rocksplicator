# Rocksplicator

[![Build Status](https://travis-ci.org/pinterest/rocksplicator.svg)](https://travis-ci.org/pinterest/rocksplicator)

Rocksplicator is a set of C++ libraries and tools for building large scale [RocksDB](http://rocksdb.org/) based stateful services. Its goal is to help application developers solve common difficulties of building large scale stateful services, such as data replication, request routing and cluster management. With Rocksplicator, application developers just need to focus on their application logics, and won't need to deal with data replication, request routing nor cluster management.

Rocksplicator includes:

 1. RocksDB replicator (a library for RocksDB real-time data replication. It supports 3 different replication modes, i.e., async replication, semi-sync replication, and sync replication.)
 2. Helix powered automated cluster management and recovery
 3. Async fbthrift client pool and fbthrift request router
 4. A stats library for maintaining & reporting server stats
 5. A set of other small tool classes for building C++ services.

## Online introduction videos
Introduction of Rocksplicator can be found in in our [presentation at 2016 Annual RocksDB meetup at FB HQ](https://www.facebook.com/TeCNoYoTTa/videos/oa.1126302657468247/10155683728004408) and [@Scale presentation](https://atscaleconference.com/videos/experimentation-at-scale-replicated-rocksdb-at-pinterest/) (starting from 17:30).

## Use cases
Currently, we have 9 different online services based on rocksplicator running at Pinterest, which consist of nearly 30 clusters, over 4000 hosts and process tens of PB data per day.

## Prerequisities

The third-party dependencies of Rocksplicator can be found in docker/Dockerfile.

## Get Started

### Install docker
Docker is used for building Rocksplicator. Follow the [Docker installation instructions](https://docs.docker.com/engine/installation/) to get Docker running on your system.

### Build docker image

You can build your own docker image (if you want to change the docker file and test it locally).
```sh
cd docker && docker build -t rocksplicator-build .
```

Or pull the one we uploaded.
```sh
docker pull pinterestbo/rocksplicator-build:latest
```

### Initialize submodules

```sh
cd rocksplicator && git submodule update --init
```

### Build the libraries & tools

Get into the docker build environment. We are assuming the rocksplicator repo is under $HOME/code/, and $HOME/docker-root is an existing directory.

```sh
docker run -v <SOURCE-DIR>:/rocksplicator -v $HOME/docker-root:/root -ti pinterestbo/rocksplicator-build:latest bash
```

Run the following command in the docker bash to build Rocksplicator:

```sh
cd /rocksplicator && mkdir -p build && cd build && cmake .. && make -j
```

### Run Tests

Run the following command in the docker bash:
```sh
cd /rocksplicator && mkdir -p build && cd build && cmake .. && make -j && make test
```

## How to build your owner service based on RocksDB replicator & cluster management libraries.
There is an example counter service under examples/counter_service/, which demonstrated a typical usage pattern for RocksDB replicator.

## Automated cluster management and recovery
Please check cluster_management directory for Helix powered automated cluster management and recovery.

## Commands for cluster management (The following is for script driven cluster management, which has been deprecated)
The cluster mangement tool rocksdb_admin.py is under rocksdb_admin/tool/.

Before using the tool, we need to generate python client code for Admin interface as follows.
```sh
cd /rocksplicator/rocksdb_admin/tool/ && ./sync.sh
```

### Create config file for a newly launched cluster.
host_file is a text file containing all hosts in the cluster. Each line is for a host in format "ip:port:zone". For example "192.168.0.101:9090:us-east-1c"
```sh
python rocksdb_admin.py new_cluster_name config --host_file=./host_file --segment=test --shard_num=1000 --overwrite
```

### Ping all hosts in a cluster
```sh
python rocksdb_admin.py cluster_name ping
```

### Remove a host from a cluster
```sh
python rocksdb_admin.py cluster_name remove_host "ip:port:zone"
```

### Promote Master for shards currenty having Slaves only
```sh
python rocksdb_admin.py cluster_name promote
```

### Add a host to a cluster
```sh
python rocksdb_admin.py cluster_name add_host "ip:port:zone"
```

### Rebalance a cluster (Evenly distribute Masters)
```sh
python rocksdb_admin.py cluster_name rebalance
```

### Load SST files from S3 to the cluster
```sh
python rocksdb_admin.py "cluster" load_sst "segment" "s3_bucket" "s3_prefix" --concurrency 64 --rate_limit_mb 64
```

## Typical cluster management workflows (This has been deprecated, please check the new Helix powered solution in the cluster_management directory)

### replacing a dead host
```sh
python rocksdb_admin.py cluster_name remove_host old_ip:old_port:zone_a
python rocksdb_admin.py cluster_name promote
python rocksdb_admin.py cluster_name add_host new_ip:new_port:zone_a
python rocksdb_admin.py cluster_name rebalance
```

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
