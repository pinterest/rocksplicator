#!/usr/bin/python
#

# Copyright 2016 Pinterest Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import argparse
import copy
import datetime
import difflib
import gevent
import gevent.monkey
from gevent.pool import Pool

from kazoo import client
from random import shuffle
import simplejson
import socket
import sys
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
import time
from sets import Set
from thrift_libs.Admin import Client
from thrift_libs.ttypes import ClearDBRequest
from thrift_libs.ttypes import BackupDBRequest
from thrift_libs.ttypes import ChangeDBRoleAndUpstreamRequest
from thrift_libs.ttypes import AddS3SstFilesToDBRequest
from thrift_libs.ttypes import GetSequenceNumberRequest
from thrift_libs.ttypes import RestoreDBRequest


"""
Script to manage any stateful services supporting the admin.thrift interface.

Supported commands:

-- Create config file for a cluster
python rocksdb_admin.py "cluster" config [options]


-- Ping all the hosts in the specified cluster
python rocksdb_admin.py "cluster" ping


-- Remove a host from the cluster. Only use it when the host has been terminated.
python rocksdb_admin.py "cluster" remove_host "ip:port:zone"


-- Promote Master for shards currently having Slaves only
python rocksdb_admin.py "cluster" promote


-- Add a host to the cluster.
python rocksdb_admin.py "cluster" add_host "ip:port:zone"


-- Rebalance the cluster (Evenly distribute Masters)
python rocksdb_admin.py "cluster" rebalance

-- Load SST files from S3 to the cluster
python rocksdb_admin.py "cluster" load_sst "segment" "s3_bucket" "s3_prefix" --concurrency 64 --rate_limit_mb 64

"""


# Overwrite the following two variables to reflect your own zk setup
CLUSTER_ZK_PATH = '/config/services/rocksdb/'
ZK_HOST_LIST = [ 'observerzookeeper010:2181',
                 'observerzookeeper011:2181',
                 'observerzookeeper012:2181',
                 'observerzookeeper013:2181',
                 'observerzookeeper014:2181',
                 'observerzookeeper015:2181',
                 'observerzookeeper016:2181',
                 'observerzookeeper017:2181',
                 'observerzookeeper018:2181',
                 'observerzookeeper019:2181',
                 'observerzookeeper020:2181',
               ]
gevent.monkey.patch_all()               


# return datetime string in format: 20150313-220328
def _datetime_str():
    return datetime.datetime.now().strftime('%Y%m%d-%H%m%S')


def LOG(msg):
    print ("[%s] %s" % (_datetime_str(), msg))


def _get_zk_client():
    c = client.KazooClient(hosts=",".join(ZK_HOST_LIST))
    c.start()
    return c


zk_client = _get_zk_client()


# return the global ZK lock for cluster
def _get_cluster_lock(cluster):
    global zk_client
    host_name = socket.gethostname()
    return zk_client.Lock(CLUSTER_ZK_PATH + cluster + '.lock', host_name)


def acquire_cluster_lock(cluster):
    LOG('Acquiring the global lock for cluster %s' % cluster)
    lock = _get_cluster_lock(cluster)
    if not lock.acquire(blocking=False):
        raise Exception('ZK lock for %s is held by someone else. Try later.' % cluster)

    return lock


def is_cluster_config_exist(cluster):
    global zk_client
    try:
        path = CLUSTER_ZK_PATH + cluster
        return zk_client.exists(path)
    except:
        LOG(sys.exc_info())
        raise


# fetch the config from ZK for cluster
def get_cluster_config(cluster):
    global zk_client
    try:
        path = CLUSTER_ZK_PATH + cluster
        config = zk_client.get(path)[0]
        return simplejson.loads(config)
    except:
        LOG(sys.exc_info())
        raise


def write_cluster_config(cluster, config):
    global zk_client
    try:
        path = CLUSTER_ZK_PATH + cluster
        raw_config = simplejson.dumps(config, indent=4, sort_keys=True)
        if not zk_client.exists(path):
            zk_client.create(path, raw_config)
        else:
            zk_client.set(path, raw_config)
    except:
        LOG(sys.exc_info())
        raise


def get_client(ip, port):
    transport = TSocket.TSocket(ip, int(port))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Client(protocol)
    transport.open()
    return client


def _ping_the_host(ip, port):
    try:
        client = get_client(ip, port)
        client.ping()
    except:
        LOG(sys.exc_info())
        raise


def _is_alive(ip, port):
    try:
        _ping_the_host(ip, port)
        return True
    except:
        return False


def _show_config_diff(old_cfg, new_cfg):
    old_json_file = simplejson.dumps(old_cfg, indent=4, sort_keys=True)
    new_json_file = simplejson.dumps(new_cfg, indent=4, sort_keys=True)

    LOG('Diff between the old config and the new config:')
    LOG('\n'.join(difflib.unified_diff(
        old_json_file.split('\n'),
        new_json_file.split('\n'),
        fromfile='old config',
        tofile='new config',
        lineterm='')))


def _get_seq_number(host, db_name):
    ip = host.split(':')[0]
    port = host.split(':')[1]
    try:
        client = get_client(ip, port)
        res = client.getSequenceNumber(GetSequenceNumberRequest(db_name))
        return res.seq_num
    except:
        LOG(sys.exc_info())
        raise


def _pick_new_master(slaves, db_name):
    largest_seq = _get_seq_number(slaves[0], db_name)
    new_master = slaves[0]
    for slave in slaves:
        if slave == new_master:
            continue

        n = _get_seq_number(slave, db_name)
        if largest_seq < n:
            largest_seq = n
            new_master = slave

    return new_master


def _promote_to_master(host, db_name):
    ip = host.split(':')[0]
    port = host.split(':')[1]
    try:
        client = get_client(ip, port)
        client.changeDBRoleAndUpStream(ChangeDBRoleAndUpstreamRequest(db_name, 'MASTER'))
    except:
        LOG(sys.exc_info())
        raise


def _change_upstream(host, new_master, db_name):
    ip = host.split(':')[0]
    port = host.split(':')[1]
    upstream_ip = new_master.split(':')[0]
    upstream_port = new_master.split(':')[1]
    try:
        client = get_client(ip, port)
        client.changeDBRoleAndUpStream(ChangeDBRoleAndUpstreamRequest(db_name, 'SLAVE', upstream_ip,
                                                                      int(upstream_port)))
    except:
        LOG(sys.exc_info())
        raise


def _demote_to_slave(host, upstream_host, db_name):
    _change_upstream(host, upstream_host, db_name)


def _find_master(cfg, seg, shard):
    for host, shards in cfg[seg].items():
        if host == 'num_shards':
            continue

        for shard_role in shards:
            if shard + ':M' == shard_role:
                return host

    raise Exception('Could not find Master for %s' % seg + shard)


def _find_slaves(cfg, seg, shard):
    slaves = []
    for host, shards in cfg[seg].items():
        if host == 'num_shards':
            continue

        for shard_role in shards:
            if shard + ':S' == shard_role:
                slaves.append(host)

    return slaves


def _count_masters(cfg, seg, host):
    n_masters = 0
    for shard in cfg[seg][host]:
        if shard.split(':')[1] == 'M':
            n_masters += 1

    return n_masters


def _load_sst_from_s3(cfg, seg, host, s3_bucket, s3_prefix,
                      s3_download_limit_mb, role):
    ip = host.split(':')[0]
    port = host.split(':')[1]
    try:
        client = get_client(ip, port)
        for shard in cfg[seg][host]:
            shard_num = shard.split(':')[0]
            if shard.split(':')[1] == role:
                db_name = '%s%s' % (seg, shard_num)
                s3_path = '%s/part-%s-' % (s3_prefix, shard_num)
                LOG('clear %s on %s' % (db_name, host))
                client.clearDB(ClearDBRequest(db_name))
                LOG('load sst from s3://%s/%s to %s on %s' % (s3_bucket, s3_path, db_name, host))
                request = AddS3SstFilesToDBRequest(db_name, s3_bucket, s3_path, s3_download_limit_mb)
                client.addS3SstFilesToDB(request)
    except:
        LOG(sys.exc_info())
        raise


def config(args):
    LOG(args)
    global zk_client
    lock = acquire_cluster_lock(args.cluster)
    try:
        LOG('Getting list of nodes in cluster %s' % args.cluster)
        with open(args.host_file) as f:
            host_and_zones = filter(None, (line.strip() for line in f))

        zone_to_hosts = {}
        for host_and_zone in host_and_zones:
            zone = host_and_zone.split(':')[-1]
            zone_to_hosts.setdefault(zone, []).append(host_and_zone)

        LOG(zone_to_hosts)
        if len(zone_to_hosts) != 3:
            raise Exception('Must have 3 zones, while %d zones found' % len(zone_to_hosts))

        if len(set(len(x) for x in zone_to_hosts.values())) > 1:
            raise Exception('Every zone should have the same # of hosts')

        segment_dict = {'num_shards': args.shard_num}

        for host_and_zone in host_and_zones:
            segment_dict[host_and_zone] = []

        current_zone = 0
        n_masters_per_zone = max(1, args.shard_num / 3)
        for hosts in zone_to_hosts.itervalues():
            begin_master_idx = current_zone * n_masters_per_zone
            if current_zone == 2:
                end_master_idx = args.shard_num
            else:
                end_master_idx = begin_master_idx + n_masters_per_zone
            shards = []
            for i in xrange(args.shard_num):
                if i >= begin_master_idx and i < end_master_idx:
                    shards.append('%05d:M' % i)
                else:
                    shards.append('%05d:S' % i)

            shuffle(shards)
            current_host = 0
            for shard in shards:
                host = hosts[current_host]
                segment_dict[host].append(shard)
                current_host = (current_host + 1) % len(hosts)

            current_zone += 1

        config_dict = {}
        config_dict[args.segment] = segment_dict

        if (not args.overwrite) and is_cluster_config_exist(args.cluster):
            current_config_dict = get_cluster_config(args.cluster)
            if args.segment in current_config_dict.keys():
                raise Exception('%s already exists in config. Aborting.' % args.segment)

            config_dict.update(current_config_dict)

        LOG(config_dict)
        LOG("Going to write the new config to zk. Proceed? (yes/no)")
        yesno = sys.stdin.readline().strip()
        if yesno != 'yes':
            raise Exception('User abort!')

        write_cluster_config(args.cluster, config_dict)

    finally:
        lock.release()


def ping(args):
    LOG(args)
    lock = acquire_cluster_lock(args.cluster)
    try:
        hosts = Set()
        cfg = get_cluster_config(args.cluster)
        for host_to_shards in cfg.values():
            for host, shards in host_to_shards.items():
                if host == 'num_shards':
                    continue
                hosts.add(host)

        for h in hosts:
            ip = h.split(':')[0]
            port = h.split(':')[1]
            _ping_the_host(ip, port)

        LOG('All hosts are good')
    finally:
        lock.release()


def remove_host(args):
    LOG(args)
    lock = acquire_cluster_lock(args.cluster)
    try:
        ip = args.ip_port_zone.split(':')[0]
        port = args.ip_port_zone.split(':')[1]
        if _is_alive(ip, port):
            raise Exception('Please terminate %s first.' % args.ip_port_zone)

        host_zone = args.ip_port_zone

        found = False
        old_cfg = get_cluster_config(args.cluster)
        new_cfg = copy.deepcopy(old_cfg)
        for host_to_shards in new_cfg.values():
            if host_zone not in host_to_shards:
                continue

            found = True
            host_to_shards.pop(host_zone)

        if not found:
            raise Exception("%s doesn't exist in the cluster" % host_zone)

        _show_config_diff(old_cfg, new_cfg)
        LOG("Going to apply the changes. Proceed? (yes/no)")
        yesno = sys.stdin.readline().strip()
        if yesno != 'yes':
            raise Exception('User abort!')

        write_cluster_config(args.cluster, new_cfg)
        LOG('Done.')
    finally:
        lock.release()


def promote(args):
    LOG(args)
    lock = acquire_cluster_lock(args.cluster)
    try:
        old_cfg = get_cluster_config(args.cluster)
        cfg = copy.deepcopy(old_cfg)
        seg_shard_slaves = {}
        for seg, host_to_shards in cfg.items():
            n_shards = host_to_shards['num_shards']
            seg_shard_slaves.setdefault(seg, {})
            for i in xrange(n_shards):
                seg_shard_slaves[seg].setdefault('%05d' % i, [])

            for host, shards in host_to_shards.items():
                if host == 'num_shards':
                    continue

                for shard in shards:
                    raw_shard = shard.split(':')[0]
                    if shard.split(':')[1] == 'M':
                        seg_shard_slaves[seg].pop(raw_shard)
                        continue

                    if raw_shard in seg_shard_slaves[seg]:
                        seg_shard_slaves[seg][raw_shard].append(host)

        LOG('Shards without Master:')
        LOG(seg_shard_slaves)
        LOG("Going to promote Master for them. Proceed? (yes/no)")
        yesno = sys.stdin.readline().strip()
        if yesno != 'yes':
            raise Exception('User abort!')

        for seg, shard_to_slaves in seg_shard_slaves.items():
            for shard, slaves in shard_to_slaves.items():
                if len(slaves) == 0:
                    continue

                new_master = _pick_new_master(slaves, seg + shard)
                _promote_to_master(new_master, seg + shard)
                cfg[seg][new_master].remove(shard + ':S')
                cfg[seg][new_master].append(shard + ':M')
                write_cluster_config(args.cluster, cfg)
                LOG('Promoted %s to be the Master for shard %s in segment %s' % (new_master, shard, seg))
                for slave in slaves:
                    if slave != new_master:
                        _change_upstream(slave, new_master, seg + shard)
                        LOG('Changed upstream for %s to be the new master.' % slave)

        _show_config_diff(old_cfg, cfg)
        LOG('Done.')
    finally:
        lock.release()


def add_host(args):
    LOG(args)
    lock = acquire_cluster_lock(args.cluster)
    try:
        ip = args.ip_port_zone.split(':')[0]
        port = args.ip_port_zone.split(':')[1]
        if not _is_alive(ip, port):
            raise Exception('Please make sure the service is running on %s.' % args.ip_port_zone)

        my_zone = args.ip_port_zone.split(':')[-1]
        old_cfg = get_cluster_config(args.cluster)
        new_cfg = copy.deepcopy(old_cfg)

        seg_to_my_shards = {}
        for seg, host_to_shards in old_cfg.items():
            n_shards = host_to_shards['num_shards']
            my_shards = []
            for i in xrange(n_shards):
                my_shards.append('%05d' % i)

            for host, shards in host_to_shards.items():
                if host == 'num_shards':
                    continue

                if host.split(':')[2] != my_zone:
                    continue

                for shard in shards:
                    my_shards.remove(shard.split(':')[0])

            if len(my_shards) == 0:
                continue

            seg_to_my_shards.setdefault(seg, my_shards)

        LOG('The new host will host these shards:')
        LOG(seg_to_my_shards)
        LOG("Proceed? (yes/no)")
        yesno = sys.stdin.readline().strip()
        if yesno != 'yes':
            raise Exception('User abort!')

        client = get_client(ip, port)
        for seg, my_shards in seg_to_my_shards.items():
            for my_shard in my_shards:
                master = _find_master(old_cfg, seg, my_shard)
                upstream_ip = master.split(':')[0]
                upstream_port = master.split(':')[1]
                upstream_client = get_client(upstream_ip, upstream_port)
                hdfs_path = '%s/%s/%s/%s/%s/%s' % (args.hdfs_dir, args.cluster, seg, my_shard, upstream_ip,
                                                   _datetime_str())
                LOG('Backup %s from %s to %s ...' % (seg + my_shard, master, hdfs_path))
                upstream_client.backupDB(BackupDBRequest(seg + my_shard, hdfs_path, args.rate_limit_mb))
                LOG('Restore %s from %s to %s ...' % (seg + my_shard, hdfs_path, master))
                client.restoreDB(RestoreDBRequest(seg + my_shard, hdfs_path, upstream_ip, int(upstream_port),
                                                  args.rate_limit_mb))
                new_cfg[seg].setdefault(args.ip_port_zone, []).append(my_shard + ':S')
                write_cluster_config(args.cluster, new_cfg)
                LOG('%s now is hosting %s' % (args.ip_port_zone, seg + my_shard))

        _show_config_diff(old_cfg, new_cfg)
        LOG('Done.')
    finally:
        lock.release()


def rebalance(args):
    LOG(args)
    lock = acquire_cluster_lock(args.cluster)
    try:
        old_cfg = get_cluster_config(args.cluster)
        new_cfg = copy.deepcopy(old_cfg)

        for seg, host_to_shards in old_cfg.items():
            n_shards = host_to_shards['num_shards']
            n_hosts = len(host_to_shards) - 1
            lower_bound = n_shards / n_hosts
            if n_shards % n_hosts == 0:
                upper_bound = n_shards / n_hosts
            else:
                upper_bound = n_shards / n_hosts + 1

            for i in xrange(n_shards):
                shard = '%05d' % i
                master = _find_master(new_cfg, seg, shard)
                slaves = _find_slaves(new_cfg, seg, shard)

                if len(slaves) == 0 or _count_masters(new_cfg, seg, master) <= upper_bound:
                    # no slaves for this shard or the master host is not overloaded
                    continue

                least_master_num = _count_masters(new_cfg, seg, slaves[0])
                least_loaded_slave = slaves[0]
                for slave in slaves:
                    n = _count_masters(new_cfg, seg, slave)
                    if n < least_master_num:
                        least_master_num = n
                        least_loaded_slave = slave

                if least_master_num >= lower_bound:
                    # no slaves are underloaded
                    continue

                # now move Master for shard from master to least_loaded_slave
                _demote_to_slave(master, master, seg + shard)
                new_cfg[seg][master].remove(shard + ':M')
                new_cfg[seg][master].append(shard + ':S')
                write_cluster_config(args.cluster, new_cfg)
                LOG('Demoted %s to be Slave for %s' % (master, seg + shard))
                old_master_seq = _get_seq_number(master, seg + shard)
                while True:
                    new_master_seq = _get_seq_number(least_loaded_slave, seg + shard)
                    if new_master_seq == old_master_seq:
                        break

                    time.sleep(0.1)

                # now the new master has catched up with the old master
                _promote_to_master(least_loaded_slave, seg + shard)
                new_cfg[seg][least_loaded_slave].remove(shard + ':S')
                new_cfg[seg][least_loaded_slave].append(shard + ':M')
                write_cluster_config(args.cluster, new_cfg)
                LOG('Promoted %s to be Master for %s' % (least_loaded_slave, seg + shard))

                # make all slaves to pull updates from the new master
                _change_upstream(master, least_loaded_slave, seg + shard)
                for slave in slaves:
                    if slave == least_loaded_slave:
                        continue

                    _change_upstream(slave, least_loaded_slave, seg + shard)

        _show_config_diff(old_cfg, new_cfg)
        LOG('Done.')
    finally:
        lock.release()


def load_sst(args):
    LOG(args)
    lock = acquire_cluster_lock(args.cluster)
    try:
        cfg = get_cluster_config(args.cluster)
        if args.segment not in cfg:
            LOG('segment %s not in cluster %s' % (args.segment, args.cluster))
        else:
            execution_pool = Pool(args.concurrency)
            # first pass load sst to masters
            for host in cfg[args.segment].keys():
                if host == 'num_shards':
                    continue
                execution_pool.spawn(_load_sst_from_s3,
                                     cfg,
                                     args.segment,
                                     host,
                                     args.s3_bucket,
                                     args.s3_prefix,
                                     args.rate_limit_mb,
                                     'M')
            execution_pool.join(raise_error=True)
            # second pass load sst to slaves
            for host in cfg[args.segment].keys():
                if host == 'num_shards':
                    continue
                execution_pool.spawn(_load_sst_from_s3,
                                     cfg,
                                     args.segment,
                                     host,
                                     args.s3_bucket,
                                     args.s3_prefix,
                                     args.rate_limit_mb,
                                     'S')
            execution_pool.join(raise_error=True)
            LOG('load sst is done')
    finally:
        lock.release()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cluster')
    subparsers = parser.add_subparsers()

    # config
    config_parser = subparsers.add_parser('config', help='config a cluster')
    config_parser.add_argument('host_file')
    config_parser.add_argument('segment')
    config_parser.add_argument('shard_num', type=int)
    config_parser.add_argument('--overwrite', dest='overwrite', action='store_true')
    config_parser.set_defaults(func=config)

    # ping
    ping_parser = subparsers.add_parser('ping', help='ping all the hosts')
    ping_parser.set_defaults(func=ping)

    # remove_host
    remove_host_parser = subparsers.add_parser('remove_host', help='remove the host from the cluster')
    remove_host_parser.add_argument('ip_port_zone')
    remove_host_parser.set_defaults(func=remove_host)

    # promote
    promote_parser = subparsers.add_parser('promote', help='promote Master for shards with Slave only')
    promote_parser.set_defaults(func=promote)

    # add_host
    add_host_parser = subparsers.add_parser('add_host', help='add the host to the cluster')
    add_host_parser.add_argument('ip_port_zone')
    add_host_parser.add_argument('-rate_limit_mb', type=int, default=50)
    add_host_parser.add_argument('-hdfs_dir', default='/rocksdb')
    add_host_parser.set_defaults(func=add_host)

    # rebalance
    rebalance_parser = subparsers.add_parser('rebalance', help='rebalance the cluster')
    rebalance_parser.set_defaults(func=rebalance)

    # load sst from s3
    load_sst_parser = subparsers.add_parser('load_sst', help='load sst files from s3')
    load_sst_parser.add_argument('segment', help='name of segment')
    load_sst_parser.add_argument('s3_bucket', help='s3 bucket name')
    load_sst_parser.add_argument('s3_prefix', help='s3 prefix')
    load_sst_parser.add_argument('--concurrency', dest='concurrency', type=int, default=128,
                                 help='maximum number of hosts concurrently loading')
    load_sst_parser.add_argument('--rate_limit_mb', dest='rate_limit_mb', type=int, default=64,
                                 help='s3 download size limit in mb')
    load_sst_parser.set_defaults(func=load_sst)

    args = parser.parse_args()
    args.func(args)
