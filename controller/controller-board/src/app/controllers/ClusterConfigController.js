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
      vm.hideCluster = false;
      vm.loadComplete = false;
      vm.statusCode = -1;
      vm.errorMessage = '';

      clusterConfigService
          .loadAllClusterNames()
          .then(function(result) {
              vm.statusCode = result.status;
              vm.clusterTable = result.data;
              vm.loadComplete = true;
          },function (error){
              vm.statusCode = error.status;
              vm.errorMessage = error.data;
              vm.loadComplete = true;
          });


      vm.selectCluster = function (cluster) {
          vm.clusterSelected = cluster;
          vm.loadComplete = false;
          clusterConfigService.setSelectedCluster(vm.clusterSelected);
          clusterConfigService.pullClusterConfig()
              .then(function(config){
                  clusterConfigService.setRawClusterConfig(config.data);
                  clusterConfigService.pullRunningHosts()
                      .then(function(hosts){
                          clusterConfigService.setRunningHosts(hosts.data);
                          clusterConfigService.processConfig();
                          vm.statusCode = hosts.status;
                          $state.go('.shard', { 'clustersName': vm.clusterSelected});
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
      }
  }
})();
