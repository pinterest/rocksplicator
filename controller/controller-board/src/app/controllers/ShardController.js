(function(){

    angular
        .module('app')
        .controller('ShardController', [
            'clusterConfigService',
            ShardController
        ]);

    function ShardController(clusterConfigService) {
        var vm = this;
        vm.loadComplete = false;
        vm.hostsInConfig = [];
        vm.hostsNotInConfig = [];
        vm.clusterSelected = clusterConfigService.getClusterName();
        vm.dataSegments = clusterConfigService.getDataSegments();
        vm.hostsInConfig = clusterConfigService.getHostInConfig();
        vm.hostsNotInConfig = clusterConfigService.getHostNotInConfig();
        vm.loadComplete = true;
    }

})();



