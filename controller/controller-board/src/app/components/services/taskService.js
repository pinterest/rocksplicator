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
                return $http.get("http://localhost:8080/v1/tasks");
            }
        };
    }
})();




