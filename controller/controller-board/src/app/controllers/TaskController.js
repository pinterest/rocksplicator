(function(){

    angular
        .module('app')
        .controller('TaskController', [
            'taskService',
            'clusterConfigService',
            TaskController
        ]);

    function TaskController(taskService, clusterConfigService) {
        var vm = this;
        vm.clusterloadComplete = false;
        vm.taskloadComplete = false;
        vm.clusterStatusCode = -1;
        vm.clusterErrorMessage = 'UNDEFINED';
        vm.taskStatusCode = -1;
        vm.taskErrorMessage = 'UNDEFINED';
        vm.namespaceClusters = [];
        vm.namespace = 'UNDEFINED';
        vm.clustername = 'UNDEFINED';
        vm.state = 'UNDEFINED';
        vm.showDetail = [];
        vm.tasks = [];
        var BAD_REQUEST = 400;
        
        vm.getDate = function(data){
            return new Date(data).toString().split(' ').splice(1,4).join(' ');
        };

        vm.getStateProgressBarValue = function (state) {
            var stateToProgressValueDict = {
                0 : 1,
                1 : 60,
                2 : 0
            };

            if (state in stateToProgressValueDict) {
                return stateToProgressValueDict[state];
            }
            else {
                return 0;
            }
        };

        vm.toggleShowDetail = function(index){
          vm.showDetail[index] = !vm.showDetail[index];
        };

        vm.getStateText = function(state){
            switch (state) {
                case 0:
                    return 'PENDING';
                case 1:
                    return 'RUNNING';
                case 2:
                    return 'FINISHED';
                case 3:
                    return 'FAILED';
            }
        };

        clusterConfigService
            .loadAllClusterNames()
            .then(function(result) {
                vm.clusterStatusCode = result.status;
                vm.namespaceClusters = result.data;
                vm.clusterloadComplete = true;
            },function (error){
                vm.statusCode = error.status;
                vm.clusterErrorMessage = error.data.message;
                vm.clusterloadComplete = true;
            });

        vm.selectTask = function () {
            vm.taskloadComplete = false;
            if (vm.state === 'UNDEFINED' || (vm.namespace === 'UNDEFINED' && vm.clustername !== 'UNDEFINED')) {
                vm.taskStatusCode = BAD_REQUEST;
                vm.taskErrorMessage = vm.state === 'UNDEFINED' ?
                    "You have to specify the task state"
                    : "You cannot have an empty namespace with non-empty clustername";
                vm.taskloadComplete = true;
                return;
            }

            taskService.getTasks(vm.namespace, vm.clustername, vm.state)
                .then(function(result) {
                    vm.taskStatusCode = result.status;
                    vm.tasks = result.data.reverse();
                    vm.showDetail = Array(vm.tasks.length).fill(false);
                    vm.taskloadComplete = true;
                }, function (error){
                    vm.taskStatusCode = error.status;
                    vm.taskErrorMessage = error.data.message;
                    vm.taskloadComplete = true;
                });
        };
    }
})();
