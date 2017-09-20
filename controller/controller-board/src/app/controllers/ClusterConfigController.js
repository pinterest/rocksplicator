(function(){

  angular
    .module('app')
    .controller('ClusterConfigController', [
      'clusterConfigService',
        '$state',
        ClusterConfigController
    ]);

  function ClusterConfigController(clusterConfigService, $state) {
      var vm = this;
      vm.clusterTable = [];
      vm.clusterSelected = '';
      vm.namespaceSelected = '';
      vm.hideCluster = false;
      vm.loadComplete = false;
      vm.statusCode = -1;
      vm.errorMessage = '';

      function getNameSpaceClusterDict(clusters) {
          var dict = {};
          for (var i = 0; i < clusters.length; i++) {
              var namespace = clusters[i].namespace;
              if (! (namespace in dict)) {
                  dict[namespace] = [];
                  dict[namespace].push(clusters[i].name);
              }
              else {
                  dict[namespace].push(clusters[i].name);
              }
          }
          return dict;
      }

      clusterConfigService
          .loadAllClusterNames()
          .then(function(result) {
              vm.statusCode = result.status;
              vm.clusters = getNameSpaceClusterDict(result.data);
              vm.clusterTable = result.data;
              vm.loadComplete = true;
          },function (error){
              vm.statusCode = error.status;
              vm.errorMessage = error.data;
              vm.loadComplete = true;
          });


      vm.selectCluster = function (namespace, cluster) {
          console.log(namespace, cluster);
          vm.namespaceSelected = namespace
          vm.clusterSelected = cluster;
          vm.loadComplete = false;
          clusterConfigService.setSelectedCluster(namespace, cluster);
          clusterConfigService.pullClusterConfig()
              .then(function(config){
                  clusterConfigService.setRawClusterConfig(config.data);
                  clusterConfigService.pullRunningHosts()
                      .then(function(hosts){
                          clusterConfigService.setRunningHosts(hosts.data);
                          clusterConfigService.processConfig();
                          vm.statusCode = hosts.status;
                          $state.go('.shard', {
                              'namespace' : vm.namespaceSelected,
                              'clustersName': vm.clusterSelected
                          });
                          vm.hideCluster = true;
                          vm.loadComplete = true;
                      },function(error){
                          vm.statusCode = error.status;
                          vm.errorMessage = error.data;
                          vm.loadComplete = true;
                      });
              },function(error){
                  vm.statusCode = error.status;
                  vm.errorMessage = error.data;
                  vm.loadComplete = true;
              });
      };
  }
})();
