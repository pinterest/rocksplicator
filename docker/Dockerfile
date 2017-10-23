#
#
#

# The base OS
FROM ubuntu:12.04

MAINTAINER Bo Liu <bol@pinterest.com>

LABEL version=0.1

ARG BUILD_JOBS=10
ENV BUILD_JOBS ${BUILD_JOBS}

RUN apt-get -q -y update && \
    apt-get -q -y install \
      python-software-properties \
      wget

# For boost 1.54.0
RUN add-apt-repository -y ppa:boost-latest/ppa && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test

# For HDFS debian libs
COPY hortonworks.gpg .
RUN wget http://public-repo-1.hortonworks.com/HDP/ubuntu12/2.1.5.0/hdp.list \
      -O /etc/apt/sources.list.d/hdp.list && \
    cat hortonworks.gpg | apt-key add -

RUN apt-get -q -y update && \
    apt-get install --force-yes -q -y \
      autoconf \
      binutils-dev \
      bison \
      curl \
      default-jdk \
      flex \
      g++-5 \
      gcc-5 \
      g++-4.7 \
      gcc-4.7 \
      gdb \
      git \
      hadoop \
      hadoop-hdfs \
      hadoop-client \
      libapr1-dev \
      libboost-context1.54-dev \
      libboost-filesystem1.54-dev \
      libboost-log1.54-dev \
      libboost-python1.54-dev \
      libboost-regex1.54-dev \
      libboost-system1.54-dev \
      libboost-thread1.54-dev \
      libboost1.54-dev \
      libboost-test1.54-dev \
      boost-program-options1.54-dev \
      libbz2-dev \
      libcap-dev \
      libcppunit-dev \
      libcurl4-openssl-dev \
      libevent-dev \
      libfftw3-dev \
      libgtest-dev \
      libhdf5-serial-dev \
      libhdfs0 \
      libhdfs0-dev \
      libkrb5-dev \
      libleveldb-dev \
      liblua5.2-dev \
      libpcap-dev \
      libsasl2-dev \
      libsnappy-dev \
      libssl-dev \
      libsvn-dev \
      libtool \
      linux-tools-generic-lts-trusty \
      make \
      scons \
      sparsehash \
      unzip \
      graphviz \
      ghostscript

# Adding Java lib path ld.so searching path configuration
RUN JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64 && \
    echo $JAVA_HOME/jre/lib/amd64/ >> /etc/ld.so.conf.d/rocksdb_admin.conf && \
    echo $JAVA_HOME/jre/lib/amd64/server >> /etc/ld.so.conf.d/rocksdb_admin.conf && \
    echo $JAVA_HOME/jre/lib/amd64/jamvm >> /etc/ld.so.conf.d/rocksdb_admin.conf

# Change the gcc/g++ aliases to point to 5
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-5 60 --slave /usr/bin/g++ g++ /usr/bin/g++-5

# numa
RUN cd /opt && \
    wget https://github.com/numactl/numactl/archive/v2.0.11.zip && \
    unzip v2.0.11.zip && \
    (cd numactl-2.0.11 && \
      ./autogen.sh && \
      ./configure && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf v2.0.11.zip numactl-2.0.11/

# newer version of cmake
RUN cd /opt && \
    wget https://cmake.org/files/v3.4/cmake-3.4.3-Linux-x86_64.tar.gz --no-check-certificate && \
    tar zxvf cmake-3.4.3-Linux-x86_64.tar.gz --strip-components=1 -C /usr/local && \
    rm cmake-3.4.3-Linux-x86_64.tar.gz

# double-conversion
RUN cd /opt && \
    wget https://github.com/google/double-conversion/archive/v1.1.5.zip && \
    unzip v1.1.5.zip && \
    (cd double-conversion-1.1.5 && \
      mkdir build && \
      cd build && \
      cmake -DBUILD_SHARED_LIBS=OFF ../ && \
      make install && \
      cmake -DBUILD_SHARED_LIBS=ON ../ && \
      make install && \
      ldconfig ) && \
    rm -rf v1.1.5.zip double-conversion-1.1.5

# jemalloc
RUN cd /opt && \
    wget https://github.com/jemalloc/jemalloc/archive/4.5.0.tar.gz && \
    tar -zxvf 4.5.0.tar.gz && \
    (cd jemalloc-4.5.0 && \
      autoconf && \
      ./configure --enable-prof && \
      make && \
# the unusual (make install; exit 0) is to ignore error for missing html doc
      (make install; exit 0) && \
      ldconfig) && \
    rm -rf 4.5.0.tar.gz jemalloc-4.5.0

# microhttpd
RUN cd /opt && \
    wget http://ftpmirror.gnu.org/libmicrohttpd/libmicrohttpd-0.9.42.tar.gz && \
    tar -zxvf libmicrohttpd-0.9.42.tar.gz && \
    (cd libmicrohttpd-0.9.42 && \
      CPPFLAGS="-gdwarf-2 -O3 -fno-omit-frame-pointer" ./configure && \
      make -j ${BUILD_JOBS}  && \
      make install) && \
    rm -rf libmicrohttpd-0.9.42.tar.gz libmicrohttpd-0.9.42

# gflags
RUN cd /opt && \
    wget https://github.com/gflags/gflags/archive/v2.0.zip && \
    unzip v2.0.zip && \
    (cd /opt/gflags-2.0/ && \
      ./configure && \
      make && make install && \
      ldconfig) && \
    rm -rf v2.0.zip gflags-2.0

# glog
RUN cd /opt && \
    wget https://github.com/google/glog/archive/v0.3.3.zip && \
    unzip v0.3.3.zip && \
    (cd /opt/glog-0.3.3/ && \
      CPPFLAGS="-gdwarf-2 -O3 -fno-omit-frame-pointer" ./configure && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf glog-0.3.3.tar.gz glog-0.3.3

# download zstd
RUN cd /opt && \
    git clone https://github.com/facebook/zstd.git && \
    (cd zstd && git reset --hard f405b8acbe8be70aa05e0a7bf035fe1efe20b99f) && \
    (cd zstd/build/cmake && \
      cmake . && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf zstd

# download folly
RUN cd /opt && \
    git clone https://github.com/facebook/folly && \
    (cd folly && \
     git reset --hard b59ee6802a2454e854a52535d31598aa967e33c0 && \
     cd folly && \
     autoreconf -ivf && \
     ./configure LDFLAGS='-ljemalloc' CXXFLAGS='-gdwarf-2 -O3 -fno-omit-frame-pointer' && \
     make -j ${BUILD_JOBS}  && \
     make install && \
     ldconfig) && \
    rm -rf folly

# wangle
RUN cd /opt && \
    git clone https://github.com/facebook/wangle && \
    (cd wangle && git reset --hard 52f08ff480931fcba1b7fa9b3eebd45d220a68de) && \
    (cd wangle/wangle && cmake . && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf wangle

# git clone https://github.com/no1msd/mstch
RUN cd /opt && \
    git clone https://github.com/no1msd/mstch && \
    (cd mstch && git reset --hard 0fde1cf94c26ede7fa267f4b64c0efe5da81a77a) && \
    (cd mstch && cmake . && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf mstch

# download fbthrift
RUN cd /opt && \
    git clone https://github.com/facebook/fbthrift && \
    (cd fbthrift && \
     git reset --hard 8e1a1e1eedbf5b551b4fe4799dab8b36267638ba && \
     cd thrift && \
     sed 's/PKG_CHECK_MODULES.*$/true/g' -i configure.ac && \
     (cd compiler && ln -sf thrifty.h thrifty.hh) && \
     autoreconf --install && \
     LDFLAGS="-ljemalloc" \
       CPPFLAGS="-gdwarf-2 -O3 -fno-omit-frame-pointer" \
       ./configure && \
     (cd compiler && make) && \
     make -j ${BUILD_JOBS} && \
     make install && \
     ldconfig) && \
    rm -rf fbthrift

# rocksdb
RUN cd /opt/ && \
    git clone https://github.com/facebook/rocksdb.git && \
    (cd rocksdb && \
     git checkout -b 5.7.fb origin/5.7.fb && \
     git reset --hard cfaeb5846bec0ac90d8da15dc11f53eafbbfd537 && \
     git cherry-pick c5f0c6cc660f1f4a8051db2aac3b8afc17818e70 && \
     git cherry-pick ba3c58cab6c691c53c7f98589651233695da1f62 && \
     sed -i -- 's/std::rindex/rindex/g' ./env/env_hdfs.cc && \
     sed -i -- '/"util\/string_util.h"/a #include "util\/logging.h"' ./env/env_hdfs.cc && \
     export CLASSPATH= && \
     for f in `find /usr/lib/hadoop-hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done && \
     for f in `find /usr/lib/hadoop | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done && \
     for f in `find /usr/lib/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done && \
     USE_SSE=1 \
     USE_HDFS=1 \
     JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64 \
     LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:/usr/lib/hadoop/lib/native \
       JEMALLOC_LIB=" /usr/lib/x86_64-linux-gnu/libjemalloc.a" \
       JEMALLOC_INCLUDE=" -I /usr/include/" \
       EXTRA_CXXFLAGS="-gdwarf-2 -std=c++1y -O3 -fno-omit-frame-pointer" \
       make -j ${BUILD_JOBS}  shared_lib && \
     make install-shared && \
     ldconfig) && \
    rm -rf rocksdb

# aws sdk
RUN cd /opt && \
    git clone https://github.com/aws/aws-sdk-cpp.git && \
    (cd aws-sdk-cpp && \
      # Rebase to : https://github.com/aws/aws-sdk-cpp/commit/39a03136ef8525bc94d82669432491e653154677
      git checkout 1.2.7 && \
      mkdir build && cd build && \
      cmake -DCMAKE_BUILD_TYPE=Release -DCUSTOM_MEMORY_MANAGEMENT=0 -DSTATIC_LINKING=1 -DBUILD_ONLY="s3" .. && \
      make -j ${BUILD_JOBS} && \
      make install && \
      rm -rf /usr/local/lib/cmake/aws-cpp-* && \
      rm -rf build && mkdir build && cd build && \
      cmake -DCMAKE_BUILD_TYPE=Release -DCUSTOM_MEMORY_MANAGEMENT=0 -DSTATIC_LINKING=0 -DBUILD_ONLY="s3" .. && \
      cd .. && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf aws-sdk-cpp

# required by rocksdb_admin.py
RUN apt-get install --force-yes -q -y python-setuptools && \
    easy_install pip && \
    pip install kazoo && \
    pip install simplejson && \
    pip install thrift && \
    pip install greenlet==0.4.7 && \
    pip install gevent==0.13.8

# install thrift
RUN cd /opt && wget http://mirrors.ocf.berkeley.edu/apache/thrift/0.9.3/thrift-0.9.3.tar.gz && \
    tar zxvf thrift-0.9.3.tar.gz && \
    cd thrift-0.9.3 && \
    ./configure --enable-shared=no --prefix=/usr/local/thrift/ && \
    make -j && \
    make install
