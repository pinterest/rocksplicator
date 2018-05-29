# The base OS
FROM ubuntu:14.04

MAINTAINER Bo Liu <bol@pinterest.com>

LABEL version=0.1

ARG BUILD_JOBS=10
ENV BUILD_JOBS ${BUILD_JOBS}


RUN apt-get -q -y update && \
    apt-get -q -y install \
      python-software-properties \
      wget \
      git \
      software-properties-common

# For gcc-5
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test

# For HDFS debian libs
RUN wget 'https://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/cloudera.list' \
    -O /etc/apt/sources.list.d/cloudera.list && \
    wget https://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key \
    -O archive.key && \
    apt-key add archive.key \
    apt-get update \
    apt-get install --force-yes -q -y hadoop-hdfs


RUN apt-get -q -y update && \
    apt-get install --force-yes -q -y \
      automake \
      autoconf \
      autoconf-archive \
      binutils-dev \
      bison \
      curl \
      default-jdk \
      flex \
      g++-4.7 \
      gcc-4.7 \
      gcc-5 \
      g++-5 \
      gcc-6 \
      g++-6 \
      gdb \
      ghostscript \
      git \
      google-perftools \
      graphviz \
      hadoop \
      hadoop-hdfs \
      hadoop-client \
      libapr1-dev \
      libboost-all-dev \
      libbz2-dev \
      libcap-dev \
      libcppunit-dev \
      libcurl4-openssl-dev \
      libdouble-conversion-dev \
      libdwarf-dev \
      libevent-dev \
      libfftw3-dev \
      libgflags-dev \
      libgtest-dev \
      libhdf5-serial-dev \
      libhdfs0 \
      libhdfs0-dev \
      libiberty-dev \
      libkrb5-dev \
      libleveldb-dev \
      liblua5.2-dev \
      liblzma-dev \
      libnuma-dev \
      libpcap-dev \
      libsasl2-dev \
      libsnappy-dev \
      libssl-dev \
      libsvn-dev \
      libtool \
      linux-tools-generic \
      make \
      openjdk-6-jdk \
      python-setuptools \
      python-pip \
      scons \
      sparsehash \
      subversion \
      unzip \
      uuid-dev \
      vim \
      zlib1g-dev


# Downgrade zookeeper to cdh5's version. trusty's default zookeeper version is 3.4.5+dfsg-1
# which is higher than cdh5's version. We need to downgrade to cdh5's version, otherwise hadoop
# package is broken.
RUN apt-get install --force-yes -q -y zookeeper=3.4.5+cdh5*

# Install awscli
RUN cd /opt && \
    wget https://s3.amazonaws.com/aws-cli/awscli-bundle.zip && \
    unzip awscli-bundle.zip && \
    ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && \
    rm awscli-bundle.zip && rm -rf awscli-bundle

# Change the gcc/g++ aliases to point to 6
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-6 60 --slave /usr/bin/g++ g++ /usr/bin/g++-6

# Set fake git credentials, otherwise git cherry-pick would throw fatal error.
RUN git config --global user.email "you@example.com" && \
    git config --global user.name "Your Name"

# LZ4
RUN cd /opt && \
    git clone https://github.com/lz4/lz4.git && \
    (cd lz4 && \
    git reset --hard c10863b98e1503af90616ae99725ecd120265dfb && \
    make && \
    make install && \
    ldconfig) && \
    rm -rf lz4

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

RUN apt-get update && apt-get install --force-yes -q -y libunwind8-dev

# Adding Java lib path ld.so searching path configuration
RUN JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64 && \
    echo $JAVA_HOME/jre/lib/amd64/ >> /etc/ld.so.conf.d/realpin.conf && \
    echo $JAVA_HOME/jre/lib/amd64/server >> /etc/ld.so.conf.d/realpin.conf && \
    echo $JAVA_HOME/jre/lib/amd64/jamvm >> /etc/ld.so.conf.d/realpin.conf

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
    wget http://ftp.gnu.org/gnu/libmicrohttpd/libmicrohttpd-0.9.42.tar.gz && \
    tar -zxvf libmicrohttpd-0.9.42.tar.gz && \
    (cd libmicrohttpd-0.9.42 && \
      CPPFLAGS="-gdwarf-2 -O3 -fno-omit-frame-pointer" ./configure && \
      make -j ${BUILD_JOBS}  && \
      make install) && \
    rm -rf libmicrohttpd-0.9.42.tar.gz libmicrohttpd-0.9.42

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
     ./configure LDFLAGS='-ljemalloc' CC=/usr/bin/gcc-5 CXX=/usr/bin/g++-5 \
     CXXFLAGS='-gdwarf-2 -O3 -fno-omit-frame-pointer' && \
     make -j ${BUILD_JOBS}  && \
     make install && \
     ldconfig) && \
    rm -rf folly

# wangle
RUN cd /opt && \
    git clone https://github.com/facebook/wangle && \
    (cd wangle && git reset --hard 52f08ff480931fcba1b7fa9b3eebd45d220a68de) && \
    (cd wangle/wangle && CC=/usr/bin/gcc-5 CXX=/usr/bin/g++-5 cmake . && \
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
     LDFLAGS="-ljemalloc" CC=/usr/bin/gcc-5 CXX=/usr/bin/g++-5 \
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
     git cherry-pick 204af1ecccb6ed8110ee04cf9130594cfcb3af27 && \
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
       JEMALLOC_LIB=" /usr/local/lib/libjemalloc.a" \
       JEMALLOC_INCLUDE=" -I /usr/local/include/" \
       EXTRA_CXXFLAGS="-gdwarf-2 -std=c++1y -O3 -fno-omit-frame-pointer" \
       make -j ${BUILD_JOBS}  shared_lib && \
     make install-shared && \
     ldconfig) && \
    rm -rf rocksdb

# farmhash
RUN cd /opt && \
    git clone https://github.com/google/farmhash.git && \
    (cd farmhash && git reset --hard 059cf991 && \
      autoreconf --install && \
      ./configure CXXFLAGS="-gdwarf-2 -O3 -fno-omit-frame-pointer" && \
      make all check && \
      make install && \
      ldconfig) && \
    rm -rf farmhash

# libprotobuf
RUN cd /opt && \
    git clone https://github.com/google/protobuf.git && \
    (cd protobuf && \
      git reset --hard b04e5cba356212e4e8c66c61bbe0c3a20537c5b9 && \
      ./autogen.sh && \
      LDFLAGS="-ljemalloc" \
        CPPFLAGS="-gdwarf-2 -O3 -fno-omit-frame-pointer" \
        ./configure && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf protobuf

# libbf (bloom filter)
# remove this once we migrate to use third_party/libbf
RUN cd /opt && \
    git clone https://github.com/mavam/libbf.git && \
    (cd libbf && \
      git reset --hard f2509db1442e8fc7c3dd5117b739886f76a4eb80 && \
      ./configure && \
      make -j ${BUILD_JOBS} && \
      make install && \
      ldconfig) && \
    rm -rf libbf

# aws sdk
RUN cd /opt && \
    git clone https://github.com/aws/aws-sdk-cpp.git && \
    (cd aws-sdk-cpp && \
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

# yaml-cpp
RUN cd /opt && \
    git clone https://github.com/jbeder/yaml-cpp && \
    (cd yaml-cpp && \
      git reset --hard 562aefc114938e388457e6a531ed7b54d9dc1b62 && \
      mkdir build && \
      cd build && \
      cmake -DBUILD_SHARED_LIBS=ON .. && \
      make -j && \
      make install && \
      cmake -DBUILD_SHARED_LIBS=OFF .. && \
      make -j ${BUILD_JOBS}&& \
      make install && \
      ldconfig) && \
    rm -rf /opt/yaml-cpp

