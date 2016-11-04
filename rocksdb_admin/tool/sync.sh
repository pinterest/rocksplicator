#!/bin/bash

BASEDIR=$(dirname $0)
pushd $BASEDIR
cp ../rocksdb_admin.thrift .
sed -i "s/namespace cpp2 admin/namespace py thrift_libs/" rocksdb_admin.thrift
/usr/local/thrift/bin/thrift --out . --gen py:new_style rocksdb_admin.thrift
sed -i "s/import TType, TMessageType, TException/import TType, TMessageType, TException, TApplicationException/" thrift_libs/Admin.py
rm -f rocksdb_admin.thrift
popd $BASEDIR
