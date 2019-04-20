import { Component, OnInit } from '@angular/core';
import { AuthService } from '../auth.service';

@Component({
  selector: 'app-auth',
  templateUrl: './auth.component.html',
  styleUrls: ['./auth.component.css']
})
export class AuthComponent implements OnInit {

  constructor(private authService: AuthService) { 
    console.log('Auth component: constructor');
  }

  ngOnInit() {
    console.log('Auth component: calling auth service to authenticate');
    this.authService.authenticate();
  }

}
