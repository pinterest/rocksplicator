(function(){
  'use strict';

  angular.module('app')
      .service('navService', [
          '$q',
          navService
  ]);

  function navService($q){
      var menuItems = [
          {
              name: 'Tasks',
              icon: 'list',
              sref: '.tasks'
          },
          {
              name: 'Clusters',
              icon: 'cloud',
              sref: '.clusters'
          },
          {
              name: 'New Task',
              icon: 'create',
              sref: '.newTask'
          },
          {
              name: 'Admin',
              icon: 'people',
              sref: '.admin'
          }
    ];

    return {
        loadAllItems : function() {
            return $q.when(menuItems);
        }
    };
  }

})();
