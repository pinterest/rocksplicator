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
                    (clustername === 'UNDEFINED' ? '': 'clusterName=' + clustername + '&')  +
                    'state=' + state;
                return $http.get(url);
            },

            replaceHost : function (namespace, clusterName, oldHost, newHost) {
                var url = 'https://controllerhttp.pinadmin.com/v1/clusters/replaceHost/'
                    + namespace + '/'+ clusterName + '?oldHost=' + oldHost + '&newHost=' + newHost + '&force=True';
                return $http.post(url);
            },
        };
    }
})();