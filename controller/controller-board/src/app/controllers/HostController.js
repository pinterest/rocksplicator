(function(){

    angular
        .module('app')
        .controller('HostController', [
            'clusterConfigService',
            HostController
        ]);

    function HostController(clusterConfigService) {
        var vm = this;
        vm.loadComplete = false;
        vm.clusterSelected = clusterConfigService.getClusterName();
        vm.dataSegments = clusterConfigService.getDataSegments();
        vm.hostsInConfig = clusterConfigService.getHostInConfig();
        vm.blacklistedHosts = clusterConfigService.getBlacklistedHosts();
        vm.candidateHosts = clusterConfigService.getCandidateHosts();
        vm.loadComplete = true;
    }

})();



