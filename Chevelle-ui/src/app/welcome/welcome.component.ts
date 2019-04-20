import { Component, OnInit } from '@angular/core';
import { AuthService } from '../auth.service';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-welcome',
  templateUrl: './welcome.component.html',
  styleUrls: ['./welcome.component.css']
})
export class WelcomeComponent implements OnInit {

  user_first_name: string;
  user_info_subscription: Subscription;

  constructor(private authService: AuthService) { 
    console.log('Welcome component constructor');
   }

  ngOnInit() {

    console.log('Welcome component OnInit');
    this.user_first_name = '';

    console.log('Welcome component: calling auth service to authenticate');
    this.authService.authenticate();

    this.user_info_subscription = this.authService.obsvUserInfo$.subscribe(
      user_info_trigg => {
        console.log('User Info trigger received by Welcome');
        this.user_first_name = this.authService.getUserfirstName();
      });

    if (this.authService.userInfo !== undefined)
    {
      this.user_first_name = this.authService.getUserfirstName();
    }

    //console.log('Welcome component OnInit');
    //this.authService.triggerGetUserInfo();

  }

}
