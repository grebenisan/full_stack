import { Component, OnInit } from '@angular/core';
import { environment } from '../environments/environment';
import { AuthService } from './auth.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit {
  title = 'Chevelle UI v2.1';
  env = environment.env;


  constructor(private authService: AuthService){}

  ngOnInit() {
    console.log('App component OnInit');
    this.authService.authInit();
  }

}
