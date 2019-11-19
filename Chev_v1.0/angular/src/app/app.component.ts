import { Component, OnInit } from '@angular/core';
import { SelectionService } from './selection.service';

import { Subscribable, Observable } from 'rxjs/Observable';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit 
{
  title = 'app';
  CHEVELLE_ENV = 'unknown';

  constructor(private selService: SelectionService) { }

  ngOnInit() {
    // this.selService.getBackendEnvironment_local().subscribe(ret_env =>   // in local mode only for testing
    this.selService.getBackendEnvironment().subscribe(ret_env =>            // in production mode
    {
      if (ret_env.CHEVELLE_ENV === 'DEV')
      {
         this.CHEVELLE_ENV = 'DEV';
      }

      if (ret_env.CHEVELLE_ENV === 'TST')
      {
         this.CHEVELLE_ENV = 'TST';
      }

      if (ret_env.CHEVELLE_ENV === 'CRT')
      {
         this.CHEVELLE_ENV = 'CRT';
      }

      if (ret_env.CHEVELLE_ENV === 'PRD')
      {
         this.CHEVELLE_ENV = 'PRD';
      }

      this.selService.CHEVELLE_ENV = this.CHEVELLE_ENV;
    });
  }












}
