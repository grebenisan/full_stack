import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';

import {ProgressBarModule} from 'primeng/progressbar';

import { SAMPLEDATA } from '../mock-sampledata';
import { SAMPLECOLUMNS } from '../mock-sampledata';

import {TimerObservable} from 'rxjs/observable/TimerObservable';

@Component({
  selector: 'app-sample-data',
  templateUrl: './sample-data.component.html',
  styleUrls: ['./sample-data.component.css']
})
export class SampleDataComponent implements OnInit, OnDestroy {


  sample_data: any[][];
  sample_columns: any[];
  record_count: number;
  column_count: number;

  subscription: Subscription;

  sub_refresh: Subscription;

  view_progress: boolean;

  empty_sample_data: any[][] = [ [{ 'c': '' }] ];


  constructor(private selService: SelectionService) { }

  ngOnInit() {
    this.record_count = 0;
    this.column_count = 0;
    this.view_progress = false;
/*    
    this.sample_data = SAMPLEDATA;
    this.sample_columns = SAMPLECOLUMNS;
    this.record_count = this.sample_data.length;

    if (( this.record_count > 0 ))
    {
      this.column_count = this.sample_data[0].length;
    } else {
      this.column_count = 0;
    }
*/
    this.subscription = this.selService.loadSampleData$.subscribe(
    new_sample_data_trig => {

      //alert('Selected table id: ' + this.selService.sel_table.Id);

      this.view_progress = true;
      this.sample_data = this.empty_sample_data;

      this.getSampleData(); // production mode
      // this.getSampleData_local(); // local testing

      // this.getSampleColumns_local(); // local testing
      // this.loading = false;
    });

    this.sub_refresh = this.selService.currentTable$.subscribe(
      new_table_id => 
      {
        this.view_progress = false;
        this.sample_data = this.empty_sample_data;
      });



  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }


  getSampleData_local(): void 
  {

/*  let timer = Observable.timer(2000,1000);
    timer.subscribe(t=> {
        // this.func(t); or do something else, like update a progress bar
    });
*/
  // let timer = TimerObservable.create(1000); // just for testing
  //  timer.subscribe(t => this.view_progress = false ); // just for testing

    if (this.selService.sel_table === undefined || this.selService.sel_table.Id === '')
    {
      this.view_progress = false;
      alert('Please select a table first to get the sample data!');
      return;
    }

    // alert('start of getSampleData_local');
    this.selService.getSampleData_local().subscribe(ret_sample_data => {
      // alert('col regexp component notified of new entity selected');
      this.sample_data = ret_sample_data;
      this.view_progress = false;
    });
    // this.loading = false;
  }

  getSampleColumns_local()
  {
    this.selService.getSampleColumns_local().subscribe(ret_sample_col_list => {
      // alert('col regexp component notified of new entity selected');
      this.sample_columns = ret_sample_col_list;
    });
  }

  getSampleData()
  {
    if (this.selService.sel_table === undefined || this.selService.sel_table.Id === '')
    {
      this.view_progress = false;
      alert('Please select a table first to get the sample data!');
      return;
    }

    this.view_progress = true;
    this.sample_data = this.empty_sample_data;

    // this.selService.getSampleData('P1', this.selService.sel_table.Location , '20').subscribe(ret_sample_data => 
    this.selService.getSampleData().subscribe(ret_sample_data => 
      {
        this.sample_data = ret_sample_data;
        this.view_progress = false;
      });


  }



}
