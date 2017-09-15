(function(){
    'use strict';

    angular.module('app')
        .service('taskService', [
            '$http',
            taskService
        ]);

    function taskService($http){

        return {
            getTasks : function(namespace, clustername, state) {
                var url = 'https://controllerhttp.pinadmin.com/v1/tasks?' +
                    (namespace === 'UNDEFINED' ? '': 'namespace=' + namespace + '&')  +
                    (clustername === 'UNDEFINED' ? '': 'clustername=' + clustername + '&')  +
                    'state=' + state;
                return $http.get(url);
            }
        };
    }
})();




