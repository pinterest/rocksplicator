(function(){
    'use strict';

    angular.module('app')
        .service('taskService', [
            '$http',
            taskService
        ]);

    function taskService($http){

        return {
            getAllTasks : function() {
                return $http.get("https://controllerhttp.pinadmin.com/v1/tasks");
            }
        };
    }
})();




