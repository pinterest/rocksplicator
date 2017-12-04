(function(){
    'use strict';

    angular.module('app')
        .service('clusterConfigService', [
            '$http',
            clusterConfigService
        ]);

    function clusterConfigService($http){
        var clusterName = '';
        var namespace = '';
        var rawClusterConfig = {};
        var runningHosts = [];
        var dataSegments = [];
        var shardConfig = {};
        var hostInConfig = [];
        var blacklistedHosts = [];
        var healthyStandbyHosts = [];

        function reset () {
            clusterName = '';
            namespace = '';
            rawClusterConfig = {};
            runningHosts = [];
            dataSegments = [];
            shardConfig = {};
            hostInConfig = [];
            blacklistedHosts = [];
            healthyStandbyHosts = []
        }

        function processConfigHelper() {
            var hostInConfigDict = {};
            rawClusterConfig.segments.forEach(function(segment) {
                // initialize segment list
                var dataSegmentDict = {};
                dataSegmentDict['name'] = segment.name;
                dataSegmentDict['numShards'] = segment.numShards;
                dataSegments.push(dataSegmentDict);

                shardConfig[segment.name] = {};
                segment.hosts.forEach(function(host) {

                    // initialize hostInConfig
                    if (! (host.ip in hostInConfigDict)) {
                        hostInConfigDict[host.ip] = 1;
                        hostInConfig.push({
                            'ip': host.ip,
                            'port': host.port,
                            'availabilityZone': host.availabilityZone
                        });
                    }

                    // initialize shardConfig
                    host.shards.forEach(function(shard) {
                        if (! (shard.id in shardConfig[segment.name])){
                            shardConfig[segment.name][shard.id] = [];
                        }

                        var replicaStatus = 'UNKNOWN';
                        if ('role' in shard){
                            replicaStatus = shard.role;
                        }
                        var hostInfo = {
                            'ip': host.ip,
                            'port': host.port,
                            'availabilityZone': host.availabilityZone,
                            'replicaStatus': replicaStatus
                        };
                        shardConfig[segment.name][shard.id].push(hostInfo);
                    });
                });
            });
        }

        return {
            loadAllClusterNames : function() {
                reset();
                return $http.get('https://controllerhttp.pinadmin.com/v1/clusters?verbose=false');
            },

            setSelectedCluster : function (ns, cluster) {
                namespace = ns;
                clusterName = cluster;
            },

            pullClusterConfig : function () {
                return $http.get('https://controllerhttp.pinadmin.com/v1/clusters/' + namespace + '/'+ clusterName);
            },

            setRawClusterConfig : function(rawConfig) {
                rawClusterConfig = rawConfig;
            },

            pullBlacklistedHosts : function () {
                return $http.get('https://controllerhttp.pinadmin.com/v1/clusters/blacklisted/' + namespace + '/'+ clusterName);
            },

            setBlacklistedHosts : function (hosts) {
                blacklistedHosts = hosts;
            },

            pullHealthyStandbyHosts : function () {
                return $http.get('https://controllerhttp.pinadmin.com/v1/clusters/hosts/'
                    + namespace + '/'+ clusterName + '?ExcludeBlacklisted=true&excludeHostsInConfig=true');
            },

            setHealthyStandbyHosts : function (hosts) {
                healthyStandbyHosts = hosts;
            },

            processConfig : function () {
                processConfigHelper();
            },

            getClusterName : function () {
                return clusterName;
            },

            getNamespace : function () {
                return namespace;
            },

            getDataSegments : function(){
                return dataSegments;
            },

            getHostInConfig : function () {
                return hostInConfig;
            },

            getBlacklistedHosts : function () {
                return blacklistedHosts;
            },

            getHealthyStandbyHosts : function () {
                return healthyStandbyHosts;
            }
        };
    }

})();




