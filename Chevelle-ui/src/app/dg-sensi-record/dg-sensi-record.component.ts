import { Component, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';

import { ChevUiService } from '../chev-ui.service';
import { DgSensitive, DG_SENSI_EMPTY } from '../dg-sensitive'; //  DG_SENSI_HEADERS


@Component({
  selector: 'app-dg-sensi-record',
  templateUrl: './dg-sensi-record.component.html',
  styleUrls: ['./dg-sensi-record.component.css']
})
export class DgSensiRecordComponent implements OnInit {

  selected_col: DgSensitive;
  subscription: Subscription;
  clear_subscription: Subscription;


  constructor(private chevUiService: ChevUiService) { }

  ngOnInit() {

    this.selected_col = DG_SENSI_EMPTY;

    this.subscription = this.chevUiService.obsvDgColSelect$.subscribe(
      dg_col_select_trigg => {
        // alert(dg_col_select_trigg.toString());
        this.selected_col = this.chevUiService.sel_dg_col;
      });

      this.clear_subscription = this.chevUiService.obsvDgSearch$.subscribe(
        dg_search_trigg => {
          this.selected_col = DG_SENSI_EMPTY;
        });
  }


  setStyleClasses() {
    let classes = {
      'sensitive': this.selected_col.dg_status === 'Protect',
      'cleared': this.selected_col.dg_status === 'Clear',
      'not_reviewed': this.selected_col.dg_status === '' || this.selected_col.dg_status === 'Pending'
    };
    return classes;
  }


  onClickSensitive()
  {
    if (this.selected_col.col_id === undefined || this.selected_col.col_id === '')
    {
      return;
    } else
    {
      this.chevUiService.triggerUpdateDgCol('sensi');
      this.selected_col.dg_status = 'Protect';
    }
  }


  onClickCleared()
  {
    if (this.selected_col.col_id === undefined || this.selected_col.col_id === '')
    {
      return;
    } else
    {
      this.chevUiService.triggerUpdateDgCol('clear');
      this.selected_col.dg_status = 'Clear';
    }
  }

  onRecommendAllPending()
  {
    // console.log('Clicking on the Apply Recommendation button');
    if ( this.chevUiService.dg_total === null || this.chevUiService.dg_total === undefined )
    {
      return;
    } else
    {
      // this.chevUiService.triggerUpdateDgCol('recommend');
      this.chevUiService.httpGetDcTholds();
    }

  }


  onProtectAllPending()
  {
    if ( this.chevUiService.dg_total === null || this.chevUiService.dg_total === undefined )
    {
      return;
    } else
    {
      this.chevUiService.triggerUpdateDgCol('protect_all');
    }

  }


  onClearAllPending()
  {
    if ( this.chevUiService.dg_total === null || this.chevUiService.dg_total === undefined )
    {
      return;
    } else
    {
      this.chevUiService.triggerUpdateDgCol('clr_all');
    }
  }

}
