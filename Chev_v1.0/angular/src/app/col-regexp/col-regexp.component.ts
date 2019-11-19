
import { Component, OnInit, OnDestroy } from '@angular/core';

import { SelectionService } from '../selection.service';
//import { Colregexp } from '../colregexp';
//import { FINCAPRVMSTR } from '../mock-colregexp';

import { ColReg } from '../colregexp';
import { COLREGLIST } from '../mock-colregexp';

// import { DataTableModule, SharedModule } from 'primeng/primeng';
import { DataTableModule } from 'primeng/datatable';
import { SharedModule } from 'primeng/shared';
// import {TableModule} from 'primeng/table';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';

@Component({
  selector: 'app-col-regexp',
  templateUrl: './col-regexp.component.html',
  styleUrls: ['./col-regexp.component.css']
})
export class ColRegexpComponent implements OnInit, OnDestroy {

  colreg_list: ColReg[];
  sel_colreg: ColReg;
  sel_colreg_id: string;
  
  //col_regexp_recs: Colregexp[];
  //selected_col_regexp: Colregexp;
  //cur_table: string;
  //cur_hv_schema: string;
  //cur_col_id: string;
  //cur_col_name: string;
  //cur_regexp_id: string;
  //cur_regexp_name: string;
  //cur_regexp_def: string;
  //cur_col_regexp_desc: string; // column to regexp association description
  //cur_dt_cq_id: string;
  loading: boolean;
  subscription: Subscription;
  sub_refresh: Subscription;

  cols: any[] = [
    { field: 'ColumnName', header: 'Column Name' },
    { field: 'RegexpName', header: 'Regexp Name' },
    { field: 'ColRegName', header: 'ColReg Name' },
    { field: 'Regexp', header: 'Regular Expression' },
    { field: 'ColumnId', header: 'Column Id' },
    { field: 'RegexpId', header: 'Regexp Id' },
    { field: 'ColRegId', header: 'ColReg Id' }
  ];

  constructor(private selService: SelectionService) { }

  ngOnInit() {
    // this.col_regexp_recs = FINCAPRVMSTR; // just for testing purposes
    
    //this.cur_col_id = '';
    //this.cur_regexp_id = '';
    //this.cur_dt_cq_id = '';

    this.loading = false;

    this.subscription = this.selService.currentTable$.subscribe(
      new_table_id => {

        // this.getListColReg_local(new_table_id); // testing mode local
        this.getListColReg(new_table_id);
        
        // this.getColRegexpAssoc_local(); // testing mode
        // this.getColRegexpAssoc(); // production mode
    });

    this.sub_refresh = this.selService.refreshColReg$.subscribe(
      refresh_trig => 
      {
        this.refreshListColReg();
      }
    );


  }

  getListColReg_local(table_id: string): void
  {
    this.loading = true;
    this.selService.getListColReg_local().subscribe(ret_colreg_list =>
    {
      this.colreg_list = ret_colreg_list;
      this.loading = false;
    });

  }


  getListColReg(table_id: string): void
  {
    this.loading = true;
    this.selService.getListColReg().subscribe(ret_colreg_list =>
    {
      this.decriptColRegList(ret_colreg_list);
      this.colreg_list = ret_colreg_list;
      this.loading = false;
    });

  }

  refreshListColReg(): void
  {
    this.loading = true;
    this.selService.getListColReg().subscribe(ret_colreg_list =>
    {
      this.decriptColRegList(ret_colreg_list);
      this.colreg_list = ret_colreg_list;
      this.loading = false;
    });

  }


  decriptColRegList(enc_colreg_list: ColReg[])
  {
    let i: number;
    for (i = 0; i < enc_colreg_list.length; i++)
    {
      enc_colreg_list[i].Regexp = this.decriptRegexp(enc_colreg_list[i].Regexp);
    }
  }

  decriptRegexp(enc_regexp: string): string
  {
    let i: number;
    let buff_string = '';
    const symbol = '©'; //'Ω'

    for (i = 0; i < enc_regexp.length; i++)
    {
      if (enc_regexp.charAt(i) === symbol)
      {
        // enc_regexp = enc_regexp.replace('Ω', '\\');
        buff_string += '\\';
      } else
      {
        buff_string += enc_regexp.charAt(i);
      }
    }
    return buff_string;
  }
/*
  getColRegexpAssoc_local(): void {
    this.loading = true;
    this.selService.getColRegexpAssoc_local().subscribe(ret_colregexp => {
      // alert('col regexp component notified of new entity selected');
      this.col_regexp_recs = ret_colregexp;
      this.loading = false;
    });
  }
*/
/*
  getColRegexpAssoc(): void {
    this.loading = true;
    this.selService.getColRegexpAssoc(this.cur_hv_schema, this.cur_table)
      .subscribe(ret_colregexp => {
        this.col_regexp_recs = ret_colregexp;
        this.loading = false;
      });
    // this.sel_col_name = '';
    // selected_entity_col = '{}'; to test this assignment
  }
*/

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  handleColRegSelect(event)
  {
    this.selService.sel_colreg = this.sel_colreg;
    this.sel_colreg_id = event.data.ColRegId;
    // alert(this.sel_colreg_id);
    this.selService.updateColReg(this.sel_colreg_id);
  }


  handleRowSelect(event) {
    this.loading = false;
    // retireve the data from the event
    //this.cur_col_name = event.data.column_name;
    //this.cur_regexp_name = event.data.regexp_name;
    //this.cur_col_regexp_desc = event.data.col_regexp_desc;
    //this.cur_regexp_def = event.data.regexp;
    //this.cur_col_id = event.data.col_id;
    //this.cur_regexp_id = event.data.reg_exp_id;
    //this.cur_dt_cq_id = event.data.dt_cq_id;

    // update the other public fields of the service
    //this.selService.sel_col_id = this.cur_col_id;
    //this.selService.sel_col_name = this.cur_col_name;
    //this.selService.sel_regexp_id = this.cur_regexp_id;
    //this.selService.sel_regexp_name = this.cur_regexp_name;
    //this.selService.sel_regexp_def = this.cur_regexp_def;
    //this.selService.sel_col_regexp_desc = this.cur_col_regexp_desc; // column to regexp association description
    //this.selService.sel_dt_cq_id = this.cur_dt_cq_id;

    // alert('current regexp id: ' + this.cur_regexp_id);
     this.selService.updateColumn(this.sel_colreg_id);
    
  }
}
