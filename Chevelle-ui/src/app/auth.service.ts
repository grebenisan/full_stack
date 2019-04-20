import { Injectable, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap, Params, Router } from '@angular/router';
import { HttpClient, HttpHeaders, HttpResponse } from '@angular/common/http';
import { Observable } from 'rxjs';
import { of } from 'rxjs';
import { Subject } from 'rxjs';
// import { JwtHelperService, JwtModule } from '@auth0/angular-jwt';
import { environment } from '../environments/environment';
import { JsonWebToken } from './JsonWebToken';

@Injectable({
  providedIn: 'root'
})
export class AuthService implements OnInit {


  // jwt_helper = new JwtHelperService();
  decodedToken: JsonWebToken;
  userInfo: any;
  user_id: string;
  ssoServiceUrl = environment.ssoServiceUrl;
  clientId = environment.clientId;
  redirectBack = environment.authRedirectBackUrl;

  redirectUrl = `${this.ssoServiceUrl}/oauth/authorize?client_id=${this.clientId}&response_type=token&redirect_uri=${this.redirectBack}`;

  private user_info_trigger = 0;
  private subjUserInfo =  new Subject<number>();
  obsvUserInfo$ = this.subjUserInfo.asObservable();

  //
  // App ID = 'http://localhost:4200/auth'
  // SSO Service URL = 'https://game.login.sys.server.domain.com'
  // SSO Authorization URL = 'https://game.login.sys.server.domain.com/oauth/authorize'
  // OIDC Discover URL = 'https://game.login.sys.server.domain.com/.well-known/openid-configuration'
  // JWT verification keys = 'https://game.login.sys.server.domain.com/token_keys'
  //

  constructor(private router: Router, private http: HttpClient, private activatedRoute: ActivatedRoute) {

    console.log('Auth Service: constructor');
    this.user_info_trigger = 0;

    if (this.getAccessToken() ) {
      console.log('Auth Service constructor: Access Token exists');
      this.decodedToken = this.parseJwt(this.getAccessToken());
      this.user_id = this.decodedToken.user_name;
      console.log(this.decodedToken);

      if (!this.localStorageHasValidToken()) {
        localStorage.removeItem('accessToken');
        this.setIsAuth(false);
        this.authenticate();
      } else {
        this.getUserInfo();
      }
    }

  }

  ngOnInit()
  {
/*
    this.user_info_trigger = 0;
    console.log('Auth Service: OnInit');

    if (this.getAccessToken() ) {
      console.log('Auth Service constructor: Access Token exists');
      this.decodedToken = this.parseJwt(this.getAccessToken());
      console.log(this.decodedToken);

      if (!this.localStorageHasValidToken()) {
        localStorage.removeItem('accessToken');
        this.setIsAuth(false);
        this.authenticate();
      } else {
        this.getUserInfo();
      }
    }
*/    
    
  }

  getAccessToken() {
    return localStorage.getItem('accessToken');
  }

  setAccessToken(token: string) {
    localStorage.setItem('accessToken', token);
  }
  
  getHeader() {
    return {headers: {'Authorization':'Bearer ' + this.getAccessToken()}};
  }

  getIsAuth(): string {
    return sessionStorage.getItem('isAuth');
  }

  setIsAuth(isAuth: boolean) {
    sessionStorage.setItem('isAuth', String(isAuth));
  }

  getGmid():string {
    return this.decodedToken.user_name.toLowerCase();
  }

  getEmail(): string {
    return this.decodedToken.email.toLowerCase();
  }

  localStorageHasValidToken(): boolean {
    if (this.getAccessToken()) {
      this.decodedToken = this.parseJwt(this.getAccessToken());
      return this.decodedToken.exp >= Date.now() / 1000;
    } else {
      return false;
    }
  }

  isAuthenticated(): boolean {
    //  return sessionStorage.getItem('isAuth') === 'true';
      return this.localStorageHasValidToken() || this.getIsAuth() === 'true';
  }

  authInit() {

    if (!this.isAuthenticated()) {
      console.log('Auth Service authInit(): not authenticated');
      this.router.navigate(['/welcome']);
    } else {
      console.log('Auth Service authInit(): already authenticated');
    }
  }

  authenticate() {
    console.log('Auth Service authenticate()');
    if (!this.isAuthenticated()) {
      this.setIsAuth(true);
      window.location.href = this.redirectUrl;
    } else if (!this.getAccessToken()) {
      this.setAccessToken(this.getParameterByNameRegex('access_token'));
      this.decodedToken = this.parseJwt(this.getAccessToken());
      this.user_id = this.decodedToken.user_name;
      console.log('Auth Service: user_id: ' + this.user_id);
      // get the user info
      // this.getUserInfo();
      this.router.navigate(['/welcome']);
      this.getUserInfo();
    }
  }

  parseJwt(token) {
    if (token) {
      var base64Url = token.split('.')[1];
      var base64 = base64Url.replace('-', '+').replace('_', '/');
      return JSON.parse(window.atob(base64));
    } else {
      return 'failure...';
    }
  }

  getUserInfo() {
    this.http.get(this.ssoServiceUrl + '/userinfo', this.getHeader())
      .subscribe(response => {
          this.userInfo = response;
          this.triggerGetUserInfo();
          console.log(this.userInfo);
        });
  }

  triggerGetUserInfo()
  {
    this.user_info_trigger++;
    this.subjUserInfo.next(this.user_info_trigger);
  }

  getUserfirstName(): string {
    if (this.userInfo.given_name !== undefined && this.userInfo.given_name !== '')
    { return this.userInfo.given_name; } else { return ''; }
  }

  hasNumber(myString) {
    return /\d/.test(myString);
  }

  getDisplayName() {
    if (this.decodedToken) {
      let firstName = this.decodedToken.email.split('.')[0];
      if (!this.hasNumber(firstName)) {
        firstName = firstName.charAt(0).toUpperCase() + firstName.slice(1);
      } else {
        firstName ="";
      }

      let lastName = this.decodedToken.email.split('.')[1];
      lastName = lastName.substring(0, lastName.length - 3);
      if (!this.hasNumber(lastName)) {
        lastName = lastName.charAt(0).toUpperCase() + lastName.slice(1);
      } else {
        lastName ="";
      }

      return `${firstName} ${lastName}`
    }
    return null;
  }

  getAccessTokenFromUrl(): string {
    return (new URL(window.location.href)).searchParams.get('access_token');
  }

  getAccessTokenFromRoute() {
    return 'null';
  }

  getQueryParam(name: string): string {
    this.activatedRoute.paramMap.subscribe((params: ParamMap) => {
      return params.get(name);
    });
    return 'param not found';
  }

  getParameterByName(name: string): string {
    let param = '';
    this.activatedRoute.queryParams.subscribe(params => {
      if (params[name] != null) {
        param = params[name];
        return param;
      }
    });
    return 'param not found';
  }

  getParameterByNameRegex(name: string): string {
    let url = window.location.href;
    console.log('Auth URL: ' + url);
    name = name.replace(/[\[\]]/g, '\\$&');
    let regex = new RegExp('[?&]' + name + '(=([^&#]*)|&|#|$)'), results = regex.exec(url);
    if (!results) { return null };
    if (!results[2]) { return '' };
    return decodeURIComponent(results[2].replace(/\+/g, ' '));
  }








}
