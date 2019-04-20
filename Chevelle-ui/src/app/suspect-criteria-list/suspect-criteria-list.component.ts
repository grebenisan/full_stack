import { Component, OnInit } from '@angular/core';
import { ChevUiService } from '../chev-ui.service';

import { Subscription } from 'rxjs';
import { SuspectCriteria, SUSPECT_CRITERIA_HEADERS, SUSPECT_CRITERIA_EMPTY, SUSPECT_CRITERIA_DATA_EMPTY } from '../suspect-criteria';

@Component({
  selector: 'app-suspect-criteria-list',
  templateUrl: './suspect-criteria-list.component.html',
  styleUrls: ['./suspect-criteria-list.component.css']
})
export class SuspectCriteriaListComponent implements OnInit {

  sc_list: SuspectCriteria[];   // suspect criteria list
  sel_sc_rec: SuspectCriteria;
  // temp_col: DgSensitive;
  sel_sc_index: number;
  loading: boolean;
  sc_data_subscription: Subscription;
  reset_subscription: Subscription;
  update_subscription: Subscription;

  //rowIndex: number;

  cols: any[] =
  [
    { field: 'data_cls_nm', header: 'Data Classification Name', width: '25%'},
    { field: 'match_type_opr', header: 'SQL match', width: '15%' },
    { field: 'match_nm', header: 'Column Name match', width: '20%' },
    { field: 'upd_by', header: 'Upd By', width: '10%' },
    { field: 'upd_ts', header: 'Upd TS', width: '15%' },
    { field: 'upd_rsn_txt', header: 'Reason', width: '15%' },
    { field: 'id', header: '', width: '0%'}

  ];




  constructor(private chevUiService: ChevUiService) { }

  ngOnInit() {
    this.sel_sc_index = null;
    this.loading = false;
    this.sc_list = null;
    this.sel_sc_rec = null;
    
    // this.sc_list = SUSPECT_CRITERIA_DATA;

    // data for suspect criteria is available now in the Service (the microservice has returned)
    this.sc_data_subscription =  this.chevUiService.obsvScInformSearch$.subscribe( sc_inform_ret => {

      // this.sc_list = null;
      // this.sc_list = this.chevUiService.getListSuspectCriteria();
      this.sel_sc_rec = null;
      this.sc_list = this.chevUiService.sc_list;
      // this.chevUiService.sc_list = null;
    });

    this.update_subscription = this.chevUiService.obsvScInformUpdate$.subscribe( sc_update_ret => {

      this.sc_list = this.chevUiService.sc_list;

      if (sc_update_ret.indexOf('update') !== -1)
      {
        //this.sc_list = this.chevUiService.sc_list;
      }

      if (sc_update_ret.indexOf('newsave') !== -1)
      {
        //this.sc_list = this.chevUiService.sc_list;
        this.sel_sc_rec = this.sc_list[0];
      }

      if (sc_update_ret.indexOf('delete') !== -1)
      {
        //this.sc_list = this.chevUiService.sc_list;
        this.sel_sc_rec = null; // removes the list index - for deletes
      }

    });

  }

  onRowSelect(event)
  {
    // alert('Curren record: ' + event.data.match_type_opr + '  ' + event.data.row_nr);
    this.chevUiService.sel_sc_rec = this.sel_sc_rec;
    this.sel_sc_index = event.index;
    //this.chevUiService.sel_sc_index = this.sel_sc_index;
    this.chevUiService.triggerSuspectCriteriaSelection();
    //console.log(this.rowIndex);
  }

}
