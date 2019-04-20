// This file can be replaced during build by using the `fileReplacements` array.
// `ng build --prod` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.

export const environment = {
  production: false,
  env: 'dev',
  // the ID for local DEV: abc...............123
  // the ID for cloud DEV: abc.................123
  clientId: 'abc.......................123',
  // auto redirect local DEV:
  // auto redirect cloud DEV: 
  authRedirectBackUrl: encodeURIComponent('http://localhost:4200'), // 'http://localhost:4200/auth'
  ssoServiceUrl: 'https://game.login.sys.server.domain.com'
};

/*
 * For easier debugging in development mode, you can import the following file
 * to ignore zone related error stack frames such as `zone.run`, `zoneDelegate.invokeTask`.
 *
 * This import should be commented out in production mode because it will have a negative impact
 * on performance if an error is thrown.
 */
// import 'zone.js/dist/zone-error';  // Included with Angular CLI.
