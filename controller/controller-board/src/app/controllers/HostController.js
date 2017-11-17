(function(){

    angular
        .module('app')
        .controller('HostController', [
            'clusterConfigService',
            'taskService',
            '$state',
            '$anchorScroll',
            '$location',
            HostController
        ]);

    function HostController(clusterConfigService, taskService, $state, $anchorScroll, $location) {
        var vm = this;
        vm.loadComplete = false;
        vm.namespace = clusterConfigService.getNamespace();
        vm.clusterSelected = clusterConfigService.getClusterName();
        vm.dataSegments = clusterConfigService.getDataSegments();
        vm.hostsInConfig = clusterConfigService.getHostInConfig();
        vm.blacklistedHosts = clusterConfigService.getBlacklistedHosts();
        vm.healthyStandbyHosts = clusterConfigService.getHealthyStandbyHosts();
        vm.loadComplete = true;
        vm.hostToReplace = "";
        vm.newHealthyHost = "";
        vm.statusCode = -1;
        vm.errorMessage = '';

        vm.selectOldHost = function (host, index) {
            vm.hostToReplace = host;
            vm.hostToReplace.index = index;
            vm.goto('selectHealthStandbyHosts');
        }

        vm.selectHealthStandbyHost = function (host, index) {
            vm.newHealthyHost = host;
            vm.newHealthyHost.index = index;
            vm.goto('confirmReplaceHost');
        }

        function flatten(host) {
            return host.ip.split('.').join('-')
                + '-' + host.port + '-' + host.availabilityZone;
        }

        vm.replaceHost = function () {
            taskService.replaceHost(vm.namespace, vm.clusterSelected,
                flatten(vm.hostToReplace), flatten(vm.newHealthyHost))
                .then(function(result){
                    vm.statusCode = result.status;
                }, function(error){
                    vm.statusCode = error.status;
                    vm.errorMessage = error.data;
                });

        }

        vm.goto = function(id) {
            $location.hash(id);
            $anchorScroll();
        };
    }

})();