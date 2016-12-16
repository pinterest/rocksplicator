# Rocksplicator

Rocksplicator is a set of C++ libraries and tools for building large scale [RocksDB](http://rocksdb.org/) based stateful services. Its goal is to help application developers solve common difficulties of building large scale stateful services, such as data replication and cluster management. With Rocksplicator, application developers just need to focus on their application logics, and won't need to deal with data replication nor cluster management. A brief introduction of Rocksplicator can be found in our [@Scale presentation](https://atscaleconference.com/videos/experimentation-at-scale-replicated-rocksdb-at-pinterest/) (starting from 17:30).

Rocksplicator includes:

 1. RocksDB replicator (a library for RocksDB real-time data replication)
 2. Cluster management library and tool for RocksDB replicator based stateful services
 3. Async fbthrift client pool and fbthrift request router
 4. A stats library for maintaining & reporting server stats
 5. A set of other small tool classes for building C++ services.

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
docker pull aliced3645/rocksplicator-build
```


### Build the libraries & tools

Get into the docker build environment. We are assuming the rocksplicator repo is under $HOME/code/, and $HOME/docker-root is an existing directory.

```sh
docker run -v <SOURCE-DIR>:/rocksplicator -v $HOME/docker-root:/root -ti rocksplicator-build bash
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

## Commands for cluster management
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

## Typical cluster management workflows

### replacing a dead host
```sh
python rocksdb_admin.py cluster_name remove_host old_ip:old_port:zone_a
python rocksdb_admin.py cluster_name promote
python rocksdb_admin.py cluster_name add_host new_ip:new_port:zone_a
python rocksdb_admin.py cluster_name rebalance
```

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
