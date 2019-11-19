import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';

import { SAMPLEDATA } from '../mock-sampledata';


@Component({
  selector: 'app-get-sample',
  templateUrl: './get-sample.component.html',
  styleUrls: ['./get-sample.component.css']
})
export class GetSampleComponent implements OnInit, OnDestroy {

  cur_entity: string;
  cur_hv_schema: string;
  cur_hdfs_loc: string;
  cur_server: string;
  cur_rec_cnt: string;

  cur_hdfs_layer: string;
  sample_data: any[][];
  sample_count: number;
  column_count: number;

  subscription: Subscription;
  load_sample_subscription: Subscription;

  loading: boolean;

  constructor(private selService: SelectionService) { }

  ngOnInit() {
    // just for testing
    this.cur_entity = '';
    this.cur_hv_schema = '';
    this.cur_hdfs_layer = '';
    this.cur_hdfs_loc = '';
    this.cur_server = 'P1';
    this.cur_rec_cnt = '20';

    this.column_count = 0;
    this.loading = true;
/*
    this.subscription = this.selService.currentEntity$.subscribe(
      new_entity => {
        this.cur_entity = new_entity;
        //this.cur_ifs = this.selService.sel_ifs;
        this.cur_hv_schema = this.selService.sel_hv_schema;
        //this.cur_ent_schema = this.selService.sel_ent_schema;

        // production mode; call the right function depending on themeta_source
        if (this.selService.cur_gdap_layer === 'IC')
        {
          this.cur_hdfs_layer = 'IC'; 
        }

        if (this.selService.cur_gdap_layer === 'edge-base')
        {
          this.cur_hdfs_layer = 'Edge-Base';
        }

    });
*/

    this.subscription = this.selService.currentTable$.subscribe(
      new_table_id => 
      {
        this.cur_entity = this.selService.sel_table.TableName;
        this.cur_hdfs_loc = this.selService.sel_table.Location;
        this.cur_hdfs_layer = this.selService.sel_table.GdapArchLayer;
      });


    this.load_sample_subscription = this.selService.loadSampleData$.subscribe(
      new_sample_data_trig => {

      });
    
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
    this.load_sample_subscription.unsubscribe();
  }



  server_src_change(srv: string): void
  {
    this.cur_server = srv;
  }

  rec_cnt_change(cnt: string): void{
    this.cur_rec_cnt = cnt;

  }

  onGetSampleData() 
  {

    /*
        this.sample_data = SAMPLEDATA;
        this.sample_count = this.sample_data.length;
    
        if (( this.sample_count > 0 ))
        {
          this.column_count = this.sample_data[0].length;
          alert('The record count in each record is: ' + this.column_count);
        } else {
          this.column_count = 0;
        }
    */
        // alert('triggering new sample data');
        this.selService.sel_server = this.cur_server;
        this.selService.sel_no_of_records = this.cur_rec_cnt;
       
        this.selService.triggerSampleData();
  }

}
