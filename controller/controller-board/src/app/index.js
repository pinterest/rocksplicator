'use strict';

angular.module('angularMaterialAdmin', ['ngAnimate', 'ngCookies',
    'ngSanitize', 'ui.router', 'ngMaterial', 'nvd3', 'app' , 'md.data.table'])
    .config(function ($stateProvider, $urlRouterProvider, $mdThemingProvider,
                      $mdIconProvider, $locationProvider) {
        $stateProvider
            .state('home', {
                url: '',
                templateUrl: 'app/views/main.html',
                controller: 'MainController',
                controllerAs: 'vm',
                abstract: true
            })
            .state('home.tasks', {
                url: '/tasks',
                controller: 'TaskController',
                controllerAs: 'vm',
                templateUrl: 'app/views/task.html'
            })
            .state('home.clusters', {
                url: '/clusters',
                controller: 'ClusterConfigController',
                controllerAs: 'vm',
                templateUrl: 'app/views/clusterConfig.html'
            })
            .state('home.clusters.shard', {
                url: '/:namespace/:clustersName',
                controller: 'HostController',
                controllerAs: 'vm',
                templateUrl: 'app/views/host.html'
            })
            .state('home.admin', {
                url: '/admin',
                templateUrl: 'app/views/admin.html'
            })
            .state('home.newTask', {
                url: '/createtask',
                templateUrl: 'app/views/newTask.html',
                controller: 'NewTaskController',
                controllerAs: 'vm'
            });

        $urlRouterProvider.otherwise('/tasks');
        $locationProvider.html5Mode(true);

        $mdThemingProvider
            .theme('default')
            .primaryPalette('grey', {
                'default': '600'
            })
            .accentPalette('teal', {
                'default': '500'
            })
            .warnPalette('defaultPrimary');

        $mdThemingProvider.theme('dark', 'default')
            .primaryPalette('defaultPrimary')
            .dark();

        $mdThemingProvider.theme('grey', 'default')
            .primaryPalette('grey');

        $mdThemingProvider.theme('custom', 'default')
            .primaryPalette('defaultPrimary', {
                'hue-1': '50'
        });

        $mdThemingProvider.definePalette('defaultPrimary', {
            '50':  '#FFFFFF',
            '100': 'rgb(255, 198, 197)',
            '200': '#E75753',
            '300': '#E75753',
            '400': '#E75753',
            '500': '#E75753',
            '600': '#E75753',
            '700': '#E75753',
            '800': '#E75753',
            '900': '#E75753',
            'A100': '#E75753',
            'A200': '#E75753',
            'A400': '#E75753',
            'A700': '#E75753'
        });

        $mdIconProvider.icon('user', 'assets/images/user.svg', 64);
  });
