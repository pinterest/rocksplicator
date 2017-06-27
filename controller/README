# Rocksplicator Controller - A Centralized Cluster Management Service

Controller is built to better monitor Rocksplicator based services. It contains the following components:
1. Controller Board. A UI that is used for monitor and checking cluster status, and manage current cluster management tasks on any cluster.
2. HTTP server. The Server accepts REST calls and either perform operation directly or pushes the task execution to the Task FIFO queue.
3. Persistent task queue. For now, we only support MySQL based task queue which persists task information in a reliable way. We
could easily support different queue implementations, for example Zookeeper based queue.
4. Worker server. The worker server runs a process which polls the task queue for pending tasks, and assign tasks to be run in a worker pool.
Workers will update back the task information for outputs when task finishes or fails.
5. A set of tasks for workers to execute. Some example tasks include 

## Get Started
Let's get started by setting up an end to end working controller system on a single node, and send a task to HTTP service, stored in MySQL
and executed by worker controller-worker.

### Prerequisite
We need to install MySQL, Zookeeper, thrift and Maven.

#### Set up MySQL

Get [MySQL installed](https://dev.mysql.com/downloads/mysql/) locally.

Create a database in MySql using root permission.
```sh
mysql -u root
create database controller;
```

Create tables needed by controller.

```sh
cd rocksplicator/controller/ && sh tools/mysql/create_schemas.sh
```

To verify tables are created successfully and check table schemas:
```sh
mysql -u root
use controller;
show tables;
describe tag;
describe task;
```

#### Set up Zookeeper

[Download](http://apache.mirrors.lucidnetworks.net/zookeeper/zookeeper-3.4.6/) and untar zookeeper.

```sh
wget http://apache.mirrors.lucidnetworks.net/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
tar -xzvf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6
cp conf/zoo_sample.cfg conf/zoo.cfg && bin/zkServer.sh start
```

Test whether it works by trying to connect:
```sh
bin/zkCli.sh -server 127.0.0.1:2181
```


#### Install Thrift & Maven.

We need thrift installed to make the Java project compile.

Mac:
```sh
brew unlink thrift
brew install https://raw.githubusercontent.com/Homebrew/homebrew-core/9d524e4850651cfedd64bc0740f1379b533f607d/Formula/thrift.rb
brew install maven
```

Linux:
```sh
sudo apt-get update -qq
sudo apt-get install maven
sudo apt-get install -q -y --force-yes libboost-dev libboost-test-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev
wget http://archive.apache.org/dist/thrift/0.9.1/thrift-0.9.1.tar.gz
tar xfz thrift-0.9.1.tar.gz
cd thrift-0.9.1 && ./configure --with-cpp=no --with-c_glib=no --with-java=yes --with-python=no --with-ruby=no --with-erlang=no --with-go=no --with-nodejs=no --with-php=no && sudo make install

```

### Build Controller

To build:
```sh
cd rocksplicator/controller
mvn clean package -DskipTests
```

To run tests:
```sh
cd rocksplicator/controller
mvn "-DfailIfNoTests=false" test
```

### Run controller

#### Start HTTP server

```sh
cd controller-http
sh bin/run.sh run
```

#### Start Worker

```sh
cd controller-worker
sh bin/run.sh run
```

#### Register a Rocksplicator cluster

Initialize the cluster.

```sh
curl -X POST http://localhost:8080/v1/clusters/initialize/my_cluster
```

Send a task which has a body to be printed out in the worker side/

```sh
curl -X POST http://localhost:8080/v1/clusters/logging/my_cluster?message=hello
```

In the worker's terminal, you should be able to see the worker claimed and executed the task:

```
11:19:57.124 [pool-4-thread-1] INFO com.pinterest.rocksplicator.controller.TaskDispatcher - Dequeued task: Task{id=2, state=1, name='com.pinterest.rocksplicator.controller.tasks.LoggingTask', clusterName='my_cluster', createdAt=Tue Jun 27 11:19:52 PDT 2017, runAfter=Tue Jun 27 11:19:52 PDT 2017, lastAliveAt=Tue Jun 27 11:19:57 PDT 2017, claimedWorker='shu-MBP13-MH0Z4', output='null'}
11:19:57.125 [pool-3-thread-2] INFO com.pinterest.rocksplicator.controller.tasks.LoggingTask - Got a message from LoggingTask: hello
11:19:57.140 [pool-3-thread-2] INFO com.pinterest.rocksplicator.controller.WorkerPool - Finished processing task com.pinterest.rocksplicator.controller.tasks.LoggingTask.
```
