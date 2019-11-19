import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';

import { Column } from '../column';
import { RegExp } from '../regexp';

@Component({
  selector: 'app-associate',
  templateUrl: './associate.component.html',
  styleUrls: ['./associate.component.css']
})
export class AssociateComponent implements OnInit, OnDestroy {

  sel_column: Column;
  sel_column_id: string;

  sel_regexp: RegExp;
  sel_regexp_id: string;

  ret_stat: string;
  // col_name: string;
  // data_type: string;
  // data_length: string;
  // data_display: string;
  // col_id: string;

  // regexp_name: string;
  // regexp_def: string;
  // regexp_id: string;
  // dt_cq_id: string;

  loading: boolean;
  col_subscription: Subscription; // subscription for new column event
  regexp_subscription: Subscription; // subscription for new regexp event

  constructor(private selService: SelectionService) { }

  ngOnInit() {

    this.sel_column = new Column;
    this.sel_regexp = new RegExp;
    this.sel_column_id = '';
    this.sel_regexp_id = '';

    this.sel_column.ColumnName = ' ';
    this.sel_column.DataType = ' ';

    this.ret_stat = '';

    this.col_subscription = this.selService.currentColumn$.subscribe(new_col_id => {
      this.sel_column = this.selService.sel_column;
      this.sel_column_id = new_col_id;

      // this.col_name = this.selService.sel_col_name;
      // this.data_type = this.selService.sel_col_type;
      // this.data_length = this.selService.sel_col_size;

      // this.data_display = this.data_type + '(' + this.data_length + ')';
      // this.col_id = this.selService.sel_col_id; property currently not available because col_id is not in ICL, only in Maxis
    });


    this.regexp_subscription = this.selService.currentRegexp$.subscribe(new_regexp_id => {
      // this.regexp_name = this.selService.sel_regexp_name;
      // this.regexp_def = this.selService.sel_regexp_def;
      // this.regexp_id = this.selService.sel_regexp_id;
      this.sel_regexp = this.selService.sel_regexp;
      this.sel_regexp_id = new_regexp_id;

      // this.regexp_name = this.selService.sel_regexp.RegexpName;
      // this.regexp_def = this.selService.sel_regexp.Regexp;
      // this.regexp_id = this.selService.sel_regexp.RegexpId;
    });


  }

  ngOnDestroy(){
    this.col_subscription.unsubscribe();
    this.regexp_subscription.unsubscribe();
  }

  onClickAssociate(someval: string)
  {

    if (this.sel_column_id === '')
    {
      alert('Select first a valid column that you want to associate with!');
      return;
    }

    if (this.sel_regexp_id === '')
    {
      alert('Select first a valid regular expression that you want to associate with!');
      return;
    }

    let assoc_name = this.sel_regexp.RegexpName + ' for ' + this.sel_column.ColumnName;

    this.selService.runAssociateColReg(this.sel_column_id, this.sel_regexp_id, assoc_name).subscribe(ret_any => 
    {
      if (ret_any.stat === 'OK')
      {
        // alert('Column to Regexp association successfull!');
        this.ret_stat = 'Successfull!';
      }
      if (ret_any.stat === 'Fail')
      {
        // alert('Column to Regexp association failed!');
        this.ret_stat = 'Failed!';
      }
      this.selService.triggerRefreshColReg();
    });

  
  }

}
