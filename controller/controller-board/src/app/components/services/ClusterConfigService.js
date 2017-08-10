(function(){
    'use strict';

    angular.module('app')
        .service('clusterConfigService', [
            '$http',
            clusterConfigService
        ]);

    function clusterConfigService($http){
        var clusterName = "UNKNOWN";
        var rawClusterConfig = {};
        var runningHosts = [];
        var dataSegments = []
        var hostsFromConfig = {};
        var shardConfig = {};
        var hostInConfig = [];
        var hostNotInConfig = [];


        function reset () {
            clusterName = "UNKNOWN";
            rawClusterConfig = {};
            runningHosts = [];
            dataSegments = []
            hostsFromConfig = {};
            shardConfig = {};
            hostInConfig = [];
            hostNotInConfig = [];
        }

        function populateConfigVariables() {
            rawClusterConfig.segments.forEach(function(segment) {
                // initialize segment list

                var dataSegmentDict = {};
                dataSegmentDict['name'] = segment.name;
                dataSegmentDict['numShards'] = segment.numShards;
                dataSegments.push(dataSegmentDict);

                shardConfig[segment.name] = {};
                segment.hosts.forEach(function(host) {

                    // initialize hostsFromConfig
                    if (! (host.ip in hostsFromConfig)) {
                        hostsFromConfig[host.ip] = {
                            "ip": host.ip,
                            "port": host.port,
                            "availabilityZone": host.availabilityZone
                        }
                    }

                    // initialize shardConfig
                    host.shards.forEach(function(shard) {
                        if (! (shard.id in shardConfig[segment.name])){
                            shardConfig[segment.name][shard.id] = [];
                        }

                        var replicaStatus = 'UNKNOWN';
                        if ("role" in shard){
                            replicaStatus = shard.role;
                        }
                        var hostInfo = {
                            "ip": host.ip,
                            "port": host.port,
                            "availabilityZone": host.availabilityZone,
                            "replicaStatus": replicaStatus
                        };
                        shardConfig[segment.name][shard.id].push(hostInfo);
                    });
                });
            } );

            for (var i = 0; i < runningHosts.length; i++){
                var ip = runningHosts[i]["config.internal_address"];
                if (ip in hostsFromConfig) {
                    var host = hostsFromConfig[ip];
                    host["name"] = runningHosts[i]["config.name"];
                    hostInConfig.push(host);
                }
                else {
                    var host = {
                        name: runningHosts[i]["config.name"],
                        ip: runningHosts[i]["config.internal_address"],
                        availabilityZone: runningHosts[i]["location"],

                    }
                    hostNotInConfig.push(host);
                }
            }
        }




        return {
            loadAllClusterNames : function() {
                reset();
                return $http.get("http://localhost:8080/v1/clusters?verbose=false");
            },

            setSelectedCluster : function (cluster) {
                clusterName = cluster;
            },

            pullClusterConfig : function () {
                return $http.get("http://localhost:8080/v1/clusters/" + clusterName);
            },

            setRawClusterConfig : function(rawConfig) {
                rawClusterConfig = rawConfig;
            },

            pullRunningHosts : function () {
                var goodClusterName = clusterName.replace(/_/g, "-");
                var url = "https://cmdb.pinadmin.com:8443/api/cmdb/getquery?fields=config.name,config.internal_address,location&query=state:running AND tags.Name:*" + goodClusterName + "*";
                return $http.post(url);
            },

            setRunningHosts : function (hosts) {
                runningHosts = hosts;
            },

            processConfig : function () {
                populateConfigVariables();
            },

            getClusterName : function () {
                return clusterName;
            },

            getDataSegments : function(){
                return dataSegments;
            },

            getHostInConfig : function () {
                return hostInConfig;
            },

            getHostNotInConfig : function () {
                return hostNotInConfig;
            }
        };
    }

})();




