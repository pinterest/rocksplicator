(function(){

    angular
        .module('app')
        .controller('TaskController', [
            'taskService',
            TaskController
        ]);

    function TaskController(taskService) {
        var vm = this;
        vm.allTasks = [];
        vm.showDetail = [];
        vm.loadComplete = false;
        vm.statusCode = -1;
        vm.errorMessage = '';

        taskService.getAllTasks()
            .then(function(result) {
                vm.statusCode = result.status;
                vm.allTasks = result.data;
                // todo: remove fake tasks below
                vm.allTasks.push({
                    "name": "com.pinterest.rocksplicator.controller.tasks.HealthCheckTask",
                    "priority": 1,
                    "body": "{\"numReplicas\":3,\"intervalSeconds\":0}",
                    "id": 13,
                    "state": 0,
                    "clusterName": "my_cluster",
                    "createdAt": 1500529716000,
                    "runAfter": 1500529716000,
                    "lastAliveAt": 1501533308000,
                    "claimedWorker": "evening-MBP13-SX0PK",
                    "output": "im just a output"
                });
                vm.allTasks.push({
                    "name": "com.pinterest.rocksplicator.controller.tasks.HealthCheckTask",
                    "priority": 0,
                    "body": "{\"numReplicas\":3,\"intervalSeconds\":0}",
                    "id": 14,
                    "state": 1,
                    "clusterName": "my_cluster",
                    "createdAt": 1500529716000,
                    "runAfter": 1500529716000,
                    "lastAliveAt": 1501533308000,
                    "claimedWorker": "evening-MBP13-SX0PK",
                    "output": null
                });
                vm.showDetail = Array(vm.allTasks.length).fill(false);
                vm.loadComplete = true;

                },function (error){
                    vm.statusCode = error.status;
                    vm.errorMessage = error.data;
                    vm.loadComplete = true;
            });


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
    }
})();
