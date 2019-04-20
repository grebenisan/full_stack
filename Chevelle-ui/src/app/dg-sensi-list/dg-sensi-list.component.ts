import { Component, OnInit } from '@angular/core';
import { ChevUiService } from '../chev-ui.service';

import { DgSensitive, DgUpdateCol, DG_SENSI_DATA_EMPTY, DG_SENSI_EMPTY } from '../dg-sensitive';  // DG_SENSI_HEADERS
import { DcThold, DcName } from '../dataclass';

import { Subscription } from 'rxjs';
import { HttpResponse } from '@angular/common/http';

@Component({
  selector: 'app-dg-sensi-list',
  templateUrl: './dg-sensi-list.component.html',
  styleUrls: ['./dg-sensi-list.component.css']
})
export class DgSensiListComponent implements OnInit {

  dg_list: DgSensitive[];
  selected_col: DgSensitive;

  // temp_col: DgSensitive;
  selected_index: number;
  loading: boolean;
  // debug_upd_trigger: string; // for debug only
  search_subscription: Subscription; // listen to a new list of DG records fron Search, Next or Previous
  // reset_subscription: Subscription;
  update_subscription: Subscription; // listen to a DG update status of a record

  cols: any[] =
  [
    { field: 'dg_status', header: 'DG Status', width: '100px'},
    { field: 'table_nm', header: 'Table name', width: '10%' },
    { field: 'column_nm', header: 'Column name', width: '10%' },
    { field: 'schema_nm',  header: 'Schema name', width: '10%' },
    { field: 'db_nm',  header: 'Database name', width: '10%' },
    { field: 'data_cls_nm', header: 'Data Class', width: '10%' },
    { field: 'prof_start_ts', header: 'Prof start', width: '10%' },
    { field: 'prof_end_ts', header: 'Prod end', width: '10%' },
    { field: 'tot_row_cnt', header: 'Total cnt', width: '5%' },
    { field: 'sample_row_cnt', header: 'Sample cnt', width: '5%' },
    { field: 'col_val_uniq_cnt', header: 'Unique cnt', width: '5%' },
    { field: 'col_val_data_cls_percent', header: 'DC percent', width: '5%' },
    { field: 'col_val_data_cls_cnt', header: 'Data Class cnt', width: '5%' },
    { field: 'col_max_val', header: 'Max', width: '5%' },
    { field: 'col_min_val', header: 'Min', width: '5%' },
    { field: 'col_avg_len', header: 'Avg', width: '5%' },
    { field: 'appl_regex_str', header: 'Applied regexp', width: '25%' },
    { field: 'col_id', header: 'Column ID', width: '10%' },
    { field: 'table_id', header: 'Table ID', width: '10%' },
    { field: 'batch_id', header: 'Batch ID', width: '5%' },
    { field: 'crt_by', header: 'Created by', width: '10%' },
    { field: 'crt_ts', header: 'Created TS', width: '10%' },
    { field: 'dg_upd_by', header: 'DG update by', width: '10%' },
    { field: 'dg_upd_ts', header: 'DG update TS', width: '10%' },
    { field: 'row_cnt', header: 'Row cnt', width: '5%' }
  ];



  constructor(private chevUiService: ChevUiService) { }

  ngOnInit() {

    this.selected_index = null;
    this.loading = false;
    this.dg_list = null;
 
    // this.debug_upd_trigger = '';
    // this.dg_list = DG_SENSI_DATA_EMPTY;

    //setTimeout(() => {
      //this.carService.getCarsSmall().then(cars => this.cars = cars);
      //this.loading = false;
    //}, 1000);

    // new list of DG records fron Search, Next or Previous
    this.search_subscription = this.chevUiService.obsvDgSearch$.subscribe(
      dg_search_trigg => {
        this.dg_list = DG_SENSI_DATA_EMPTY;
        this.selected_index = null;
        this.selected_col = DG_SENSI_EMPTY;
        //alert(dg_search_trigg.toString());
        this.getListDgSensitive();
        // this.chevUiService.triggerInformSearch();

        // alert('Tot cnt: ' + this.chevUiService.dg_total_cnt.toString());
      });

    // a new DG status update for a particular record, or for a Recommendation
    this.update_subscription = this.chevUiService.obsvDgColUpdate$.subscribe(
      dg_update_trigg => {

        // this.debug_upd_trigger = dg_update_trigg.toString();

      //  if (this.selected_index === null  )  {
      //    alert('No index yet'); // check situations when this index is null but the DG screen is loaded
      //    return;
      //  }
      // alert('Trigger in dg-sensi-list is: ' + dg_update_trigg.toString());
        

        if (dg_update_trigg.indexOf('sensi') !== -1)
        {
          if ( this.selected_index === null || this.selected_index === undefined )  {
            // alert('Select first a record to review!'); // check situations when this index is null but the DG screen is loaded
            console.log('Select first a record to review!');
            return;
          } else
          {
            this.updateDgStatus('Protect');
            // this.debug_upd_trigger = dg_update_trigg.toString();
          }
        }

        if (dg_update_trigg.indexOf('clear') !== -1)
        {
          if ( this.selected_index === null || this.selected_index === undefined )  {
            // alert('Select first a record to review!'); // check situations when this index is null but the DG screen is loaded
            console.log('Select first a record to review!');
            return;
          } else
          {
            this.updateDgStatus('Clear');
            // this.debug_upd_trigger = dg_update_trigg.toString();
          }
        }

        if (dg_update_trigg.indexOf('recommend') !== -1)
        {
          // scan all the dg_status fields and if "Pending" change them to Protect or Clear depending on the dc_percent value
          this.recommendDgStatus();

        }

        if (dg_update_trigg.indexOf('protect_all') !== -1)
        {
          this.ProtectAllDgStatus();
        }

        if (dg_update_trigg.indexOf('clr_all') !== -1)
        {
          this.ClearAllDgStatus();
        }
                // inform the dg-sensi-search about the latest pending counts
        this.chevUiService.triggerInformSearch();


      }
    );

  }

  setStyleClasses(sensi: string) {
    const classes = {
      'sensitive': sensi === 'Protect',
      'cleared': sensi === 'Clear',
      'not_reviewed': sensi === '' || sensi === 'Pending'
    };
    return classes;
  }

  setColStyleClases(col_name: string) {
     let col_classes = {
    'regexp_width': col_name === 'appl_regex_str',
    'dg_width': col_name === 'dg_status',
    'default_width': col_name !== 'appl_regex_str' && col_name !== 'dg_status'
    };
    return col_classes;
  }

  onRowSelect(event)
  {
    // alert('COl ID selected: ' + event.data.col_id);
    // alert('Index : ' + event.index);
    // alert('Current col ID: ' + this.selected_col.col_id);
    this.selected_index = event.index;
    this.loading = false;
    this.chevUiService.sel_dg_col = this.selected_col;
    // this.temp_col = this.cloneDg(this.selected_col);
    this.chevUiService.triggerSelectDgCol();
  }

  getListDgSensitive(): void {
    this.dg_list = null;
    this.loading = true;

    this.chevUiService.getListDgSensitive().subscribe(
      (ret_dg_sensitive: HttpResponse<DgSensitive[]>) =>
      {
        // get the headers
        // console.log('Here are the headers for DG Gov review:');
        // console.log('Content-type: ' + ret_dg_sensitive.headers.get('content-type'));
        // console.log('dg_pending_cnt: ' + ret_dg_sensitive.headers.get('dg_pending_cnt'));
        // console.log('dg_total_cnt: ' + ret_dg_sensitive.headers.get('dg_total_cnt'));

        this.chevUiService.dg_pending_total = Number(ret_dg_sensitive.headers.get('dg_pending_cnt'));
        this.chevUiService.dg_total = Number(ret_dg_sensitive.headers.get('dg_total_cnt'));

        this.dg_list = ret_dg_sensitive.body;

        // calculate the number of total pending and page pending records
        this.computePagePendingCnt();
        this.computeTotalPendingMsg();
        this.chevUiService.dg_pending_over = this.chevUiService.dg_pending_total - this.chevUiService.dg_pending_page;

        this.loading = false;

        this.chevUiService.triggerInformSearch();

        // decode the regular expressions
        let i: number;
        let encoded_regexp: string;

        for (i = 0; i < this.dg_list.length; i++)
        {
          if ( this.dg_list[i].appl_regex_str === null || this.dg_list[i].appl_regex_str === undefined )
          {
            this.dg_list[i].appl_regex_str = '';
          } else {
            encoded_regexp = this.dg_list[i].appl_regex_str.valueOf();
            this.dg_list[i].appl_regex_str = this.decriptRegexp(encoded_regexp);
          }
        }

      }
    );

  }

  cloneDg(dg: DgSensitive): DgSensitive {
    let local_dg = new DgSensitive();
    for (let prop in dg) 
    {
      local_dg[prop] = dg[prop];
    }
    return local_dg;
  }

  updateDgStatus(dg_status: string)
  {
    this.loading = true;

    let local_dg_list = [...this.dg_list];
    local_dg_list[this.selected_index] = this.cloneDg(this.selected_col);
    local_dg_list[this.selected_index].dg_status = dg_status;
    this.dg_list = local_dg_list;

    this.computePagePendingCnt();
    this.chevUiService.dg_pending_total = this.chevUiService.dg_pending_over + this.chevUiService.dg_pending_page;
    this.computeTotalPendingMsg();


    let local_upd_col = this.convertDgColToUpdateMsg(this.selected_col, dg_status);

    this.chevUiService.httpDgReviewUpdateCol(local_upd_col).subscribe( ret => {
      this.loading = false;
      local_upd_col = null; // garbage collector will clean-up
    });

  }

  convertDgColToUpdateMsg(dg_col: DgSensitive, dg_status: string): DgUpdateCol
  {
    let upd_col = new DgUpdateCol();

    if (dg_status === 'Protect')
    {
      upd_col.dg_status = '20';
    } else
    if (dg_status === 'Clear')
    {
      upd_col.dg_status = '21';
    } else {
      upd_col.dg_status = '20';  // protect by default
    }

    // console.log('convertDgColToUpdateMsg: ' + dg_status);

    upd_col.data_cls_nm = dg_col.data_cls_nm.valueOf();
    upd_col.col_id = dg_col.col_id.valueOf();
    upd_col.table_id = dg_col.table_id.valueOf();
    upd_col.crt_ts = dg_col.crt_ts.valueOf();
    upd_col.dg_upd_by = 'chevelle_ui';
    upd_col.batch_id = Number(dg_col.batch_id).valueOf();

    return upd_col;
  }

  computeFromTo()
  {
    if ( this.chevUiService.dg_total_cnt < this.chevUiService.sel_page_size )
    {
      this.chevUiService.dg_page_to = this.chevUiService.dg_total_cnt;
    }

  }

  computeDGPendingCnt()
  {
    let local_pending = 0;
    let i: number;

    
    for (i = 0; i < this.dg_list.length; i++)
    {
      if (this.dg_list[i].dg_status === 'Pending' || this.dg_list[i].dg_status === '' )
      {
        local_pending++;
      }
    }

    this.chevUiService.dg_pending_cnt = local_pending;
  
    if ( this.chevUiService.dg_pending_cnt === 0 )
    {
      this.chevUiService.dg_pending_str = 'All records for this DB have been reviewed!';
    } 

    if ( this.chevUiService.dg_pending_cnt > 0 )
    {
      this.chevUiService.dg_pending_str = local_pending.toString() + ' records pending review!';
    }     


  }

  computePagePendingCnt()
  {
    // compute the pending count for the current page (dg_pending_page)
    let local_pending = 0;
    let i: number;

    
    for (i = 0; i < this.dg_list.length; i++)
    {
      if (this.dg_list[i].dg_status === 'Pending' || this.dg_list[i].dg_status === '' )
      {
        local_pending++;
      }
    }
    this.chevUiService.dg_pending_page = local_pending;

    // compute the pending count message for the current page (dg_pending_page_msg)
    if ( this.chevUiService.dg_pending_page === 0 )
    {
      this.chevUiService.dg_pending_page_msg = 'All records on this page have been reviewed!';
    } 

    if ( this.chevUiService.dg_pending_page > 0 )
    {
      this.chevUiService.dg_pending_page_msg = local_pending.toString() + ' pending on this page!';
    }

  }

  computeTotalPendingMsg()
  {
    if ( this.chevUiService.dg_pending_total > 0 )
    {
      this.chevUiService.dg_pending_total_msg = this.chevUiService.dg_pending_total.toString() + ' pending total!';
    } else {
      this.chevUiService.dg_pending_total_msg = 'All records for this DB have been reviewed!';
    }  
  }

  decriptRegexp(enc_regexp: string): string
  {
    let i: number;
    let buff_string = '';
    // const symbol = '©'; //
    const char_169 = String.fromCharCode(169);  // the copyright character "©" - ascii decimal 169
    const char_171 = String.fromCharCode(171);  // left double angle quotes character "«" - ascii decimal 171 
    const char_187 = String.fromCharCode(187);  // right double angle quotes char "»" - ascii decimal 187 

    const backslash_char = String.fromCharCode(92); // the backslash char "\" - ascii decimal 92
    const open_brace_char = String.fromCharCode(123); // opening brace char “{“ - ascii decimal  123
    const close_brace_char = String.fromCharCode(125);  // closing brace char "}" - ascii decimal  125

    for (i = 0; i < enc_regexp.length; i++)
    {
      if (enc_regexp.charAt(i) === char_169)
      {
        // enc_regexp = enc_regexp.replace(char_169, backslash_char);
        buff_string += backslash_char;
      } else
      if (enc_regexp.charAt(i) === char_171)
      {
        // enc_regexp = enc_regexp.replace(char_171,  open_brace_char);
        buff_string +=  open_brace_char;
      } else
      if (enc_regexp.charAt(i) === char_187)
      {
        // enc_regexp = enc_regexp.replace(char_171,  open_brace_char);
        buff_string +=  close_brace_char;
      } else
      {
        buff_string += enc_regexp.charAt(i);
      }
    }
    return buff_string;
  }


  findTholdForDcName(dc_name: string)
  {

  }



  recommendDgStatus(): void
  {
    if (this.dg_list === null || this.dg_list === undefined)
    {
      return; // nothing to recommend on
    }

    if (this.dg_list.length === 0)
    { return; }

    this.loading = true;

    let upd_list = new Array<DgUpdateCol>();
    let upd_col: DgUpdateCol;
    let i: number;
    let recommend_thold = Number(50.0).valueOf();

    for (i = 0; i < this.dg_list.length; i++)
    {
      if (this.dg_list[i].dg_status === 'Pending' || this.dg_list[i].dg_status === '' )
      {
        recommend_thold = this.chevUiService.getTholdByDcName(this.dg_list[i].data_cls_nm);
        this.dg_list[i] = this.cloneDg(this.dg_list[i]);
        if ( this.dg_list[i].col_val_data_cls_percent >= recommend_thold )
        {
          this.dg_list[i].dg_status = 'Protect';
        } else
        {
          this.dg_list[i].dg_status = 'Clear';
        }

        upd_col = this.convertDgColToUpdateMsg(this.dg_list[i], this.dg_list[i].dg_status);
        upd_list.push(upd_col);
      }
    }

    this.chevUiService.dg_pending_page = 0;
    this.chevUiService.dg_pending_page_msg = 'All records on this page have been reviewed!';

    this.chevUiService.dg_pending_total = this.chevUiService.dg_pending_over;
    this.computeTotalPendingMsg();

    // console.log('Total to recommend: ' + upd_list.length.toString());
    // console.log(upd_list);

    if (upd_list.length > 0)
    {
      // call /datagov/updatebatch
      this.chevUiService.httpDgReviewUpdateBatch(upd_list).subscribe(ret => {
        this.loading = false;
        upd_list = null; // garbage collector will clean-up
      });
    } else {
      this.loading = false;
      upd_list = null;
    }

  }

  ProtectAllDgStatus(): void
  {
    if (this.dg_list === null || this.dg_list === undefined)
    {
      return; // nothing to recommend on
    }

    let i: number;
    this.loading = true;
    // create an array of DgUpdateCol, each one with the copy of it's dg_list equivalent
    let upd_list = this.convertDgColToUpdateList(this.dg_list, 'Protect');

    // console.log('Protect All count: ' + upd_list.length.toString());

    for (i = 0; i < this.dg_list.length; i++)
    {
      if (this.dg_list[i].dg_status === 'Pending' || this.dg_list[i].dg_status === '' )
      {
        this.dg_list[i].dg_status = 'Protect';
        this.dg_list[i] = this.cloneDg(this.dg_list[i]);
      }
    }

    this.chevUiService.dg_pending_page = 0;
    this.chevUiService.dg_pending_page_msg = 'All records on this page have been reviewed!';

    this.chevUiService.dg_pending_total = this.chevUiService.dg_pending_over;
    this.computeTotalPendingMsg();

    if (upd_list.length > 0)
    {
      // call /datagov/updatebatch
      this.chevUiService.httpDgReviewUpdateBatch(upd_list).subscribe(ret => {
        this.loading = false;
        upd_list = null; // garbage collector will clean-up
      });
    } else {
      this.loading = false;
      upd_list = null;
    }
    // clean-up

  }


  convertDgColToUpdateList(dg_list: DgSensitive[], dg_status: string): DgUpdateCol[]
  {
    let upd_list = new Array<DgUpdateCol>();
    let i: number;
    let upd_col: DgUpdateCol;



    for (i = 0; i < dg_list.length; i++)
    {
      if (dg_list[i].dg_status === 'Pending')
      {
        upd_col = this.convertDgColToUpdateMsg(dg_list[i], dg_status);
        upd_list.push(upd_col);
      }
    }

    return upd_list;
  }


  ClearAllDgStatus(): void
  {
    if (this.dg_list === null || this.dg_list === undefined)
    {
      return; // nothing to recommend on
    }

    let i: number;
    this.loading = true;
    // create an array of DgUpdateCol, each one with the copy of it's dg_list equivalent
    let upd_list = this.convertDgColToUpdateList(this.dg_list, 'Clear');

    // console.log('Clear All count: ' + upd_list.length.toString());

    for (i = 0; i < this.dg_list.length; i++)
    {
      if (this.dg_list[i].dg_status === 'Pending' || this.dg_list[i].dg_status === '' )
      {
        this.dg_list[i].dg_status = 'Clear';
        this.dg_list[i] = this.cloneDg(this.dg_list[i]);
      }
    }

    this.chevUiService.dg_pending_page = 0;
    this.chevUiService.dg_pending_page_msg = 'All records on this page have been reviewed!';

    this.chevUiService.dg_pending_total = this.chevUiService.dg_pending_over;
    this.computeTotalPendingMsg();

    if (upd_list.length > 0)
    {
      // call /datagov/updatebatch
      this.chevUiService.httpDgReviewUpdateBatch(upd_list).subscribe(ret => {
        this.loading = false;
        upd_list = null; // garbage collector will clean-up
      });
    } else {
      this.loading = false;
      upd_list = null;
    }

  }


}

