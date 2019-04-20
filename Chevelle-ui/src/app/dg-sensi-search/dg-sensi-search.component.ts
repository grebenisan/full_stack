import { Component, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';
// import { ProgressBarModule } from 'primeng/progressbar';
import {User} from '../user';
import { DcThold, DcName } from '../dataclass';
import { ChevUiService } from '../chev-ui.service';


@Component({
  selector: 'app-dg-sensi-search',
  templateUrl: './dg-sensi-search.component.html',
  styleUrls: ['./dg-sensi-search.component.css']
})
export class DgSensiSearchComponent implements OnInit {

  db_name: string;
  // asms_id: string;
  filter_by: string;
  //filter_enable: boolean;
  page_size: number;
  page_offset: number;  // to be removed
  page_from: number;
  page_to: number;
  dg_total: number;
  dg_pending_page: number;
  dg_pending_total: number;
  // dg_pending_text: string;
  dg_pending_page_msg: string;
  dg_pending_total_msg: string;
  dg_page_text: string;
  running: boolean;
  inform_subscription: Subscription;
  electra_disabled: boolean;

  constructor( private chevUiService: ChevUiService ) { }

  ngOnInit() {
    this.db_name = '';
    // this.asms_id = '';
    this.filter_by = 'db';
    this.page_offset = 1; // to be removed

    this.page_from = 1;
    this.page_to = 10;
    this.page_size = 10;

    this.dg_total = 0;
    this.dg_pending_page = 0;
    this.dg_pending_total = 0;
    // this.dg_pending_text = 'Search a database for columns to be reviewed!';
    this.dg_pending_page_msg = '';
    this.dg_pending_total_msg = 'Search a database for columns to be reviewed!';
    this.dg_page_text = '1 to 10';
    this.running = false;
    this.electra_disabled = true;

    this.inform_subscription = this.chevUiService.obsvDgInformSearch$.subscribe(
      dg_inform_trigg => {

        this.dg_total =  this.chevUiService.dg_total;
        this.dg_pending_total = this.chevUiService.dg_pending_total;
        this.dg_pending_page = this.chevUiService.dg_pending_page;

        if ( this.page_to > this.chevUiService.dg_total )
        {
          // this needs to be computed at the end of the range when the size is not equal with the standard size
          this.page_to = this.chevUiService.dg_total;
          this.chevUiService.dg_page_size = this.page_to - this.page_from + 1;
        } else {
          this.chevUiService.dg_page_size = this.page_size;
        }
        
        // this.chevUiService.dg_page_from = this.page_from;
        this.chevUiService.dg_page_to = this.page_to;

        this.computePendingMsg();

        

        this.ElectraIsDisabled();


      //  if (this.selected_index === null  )  {
      //    alert('No index yet');
      //    return;
      //  }
/*
        if (dg_update_trigg.indexOf('sensi') !== -1)
        {
          //alert('sensitive from Search');
          //this.updateDgStatus('Sensitive');
        }

        if (dg_update_trigg.indexOf('clear') !== -1)
        {
          //alert('cleared from Search');
          //this.updateDgStatus('Cleared');
        }
*/
        // compute the messages for the pending review records
/*        if ( this.chevUiService.dg_pending_cnt === 0 )
        {
          this.chevUiService.dg_pending_str = 'All records for this DB have been reviewed!';
          this.dg_pending_total_msg = 'All records for this DB have been reviewed!';
        }

        if ( this.chevUiService.dg_pending_cnt > 0 )
        {
          this.chevUiService.dg_pending_str = this.chevUiService.dg_pending_cnt.toString() + ' records pending review!';
          this.dg_pending_total_msg = this.chevUiService.dg_pending_str;
        }
*/

      }
    );


  }

  computePendingMsg()
  {
    // compute the pending count message for the current page (dg_pending_page_msg)
    if ( this.chevUiService.dg_pending_page > 0 )
    {
      this.chevUiService.dg_pending_page_msg = this.chevUiService.dg_pending_page.toString() + ' pending on this page!';
    } else {
      this.chevUiService.dg_pending_page_msg = 'All records on this page have been reviewed!';
    }

    // compute the total pending count message (dg_pending_total_msg)
    if ( this.chevUiService.dg_pending_total > 0 )
    {
      this.chevUiService.dg_pending_total_msg = this.chevUiService.dg_pending_total.toString() + ' pending total!';
    } else {
      this.chevUiService.dg_pending_total_msg = 'All records for this DB have been reviewed!';
    }  

    this.dg_pending_page_msg = this.chevUiService.dg_pending_page_msg;
    this.dg_pending_total_msg = this.chevUiService.dg_pending_total_msg;

  }

  onDbNameChange()
  {
    this.chevUiService.sel_db_name = this.db_name;

    // this.chevUiService.sel_page_size = this.page_size;
    // this.chevUiService.sel_page_offset = this.page_offset;
  }

  page_size_change(new_size: number)
  {
    this.page_size = new_size;

    // this.chevUiService.sel_db_name = this.db_name;
    this.chevUiService.sel_page_size = this.page_size;    // to be removed
    this.chevUiService.sel_page_offset = this.page_offset; // to be removed
    this.chevUiService.dg_page_size = this.page_size;

    if (this.dg_total === 0)
    {
      this.page_from = 1;
      this.page_to = this.page_size;
      this.chevUiService.dg_page_from = this.page_from;
      this.chevUiService.dg_page_to = this.page_to;
    } else { // now we have some data 
      this.chevUiService.dg_page_from = this.page_from;
      this.chevUiService.dg_page_to = this.page_to;
    }

  }

  /*
  onFilterBy(filter_by: string)
  {
    if ( filter_by === 'asms_id' )
    {
      this.filter_by = 'asms';
    }
    if( filter_by === 'db_name' )
    {
      this.filter_by = 'db';
    }
    //alert('Filter by: ' + this.filter_by);
  }
*/

  isDisabled(): boolean {
    if ( this.filter_by === 'asms' )
    {
      return true;
    } else if( this.filter_by === 'db' )
    {
      return false;
    } else
    {
      return false;
    }
  }

  ElectraIsDisabled()
  {

    if ( this.dg_pending_total > 0 )
    {
      this.electra_disabled = true;
    } else 
    {
      this.electra_disabled = false;
    }

  }

  onSignalElectra()
  {
    alert('This feature will be implemented on a future release!');
  }



  onClickSearch() // click on the "Search" button, always start from the the first record
  {
    // if the dbname is empty, or (arbitrarily short) , then exit
    if (this.db_name === '' || this.db_name.length < 3)
    {
      alert('Enter a valid DB name before searching');
      return;
    }

    this.page_offset = 1; // to be deleted
    this.page_from = 1; // the Search function always starts from the beginning
    this.page_to = this.page_size;

    this.chevUiService.sel_db_name = this.db_name;  // to be deleted
    this.chevUiService.sel_page_size = this.page_size;  // to be deleted
    this.chevUiService.sel_page_offset = this.page_offset; // to be deleted

    this.chevUiService.dg_db_name = this.db_name;
    this.chevUiService.dg_page_from = this.page_from;
    this.chevUiService.dg_page_to = this.page_to;
    this.chevUiService.dg_page_size = this.page_size;


    this.chevUiService.triggerDgSearch();
  }

  onClickNext()
  {
    if ( this.chevUiService.dg_total === null || this.chevUiService.dg_total === undefined )
    {
      return;
    }

    if ( this.page_to === this.chevUiService.dg_total )
    {
      return;
    }

    this.page_from = this.page_to + 1;
    this.page_to = this.page_to + this.page_size;
    if ( this.page_to > this.chevUiService.dg_total )
    {
      // this needs to be computed at the end of the range when the size is not equal with the standard size
      this.page_to = this.chevUiService.dg_total;
      this.chevUiService.dg_page_size = this.page_to - this.page_from + 1;
    } else {
      this.chevUiService.dg_page_size = this.page_size;
    }

    this.chevUiService.dg_db_name = this.db_name;
    this.chevUiService.dg_page_from = this.page_from;
    this.chevUiService.dg_page_to = this.page_to;

    this.chevUiService.triggerDgSearch();

  }
  
  onClickPrevious()
  {
    if ( this.chevUiService.dg_total === null || this.chevUiService.dg_total === undefined )
    {
      return;
    }

    if ( this.page_from === 1 )
    {
      return;
    }
    
    // this.page_from = this.page_to + 1;
    // this.page_to = this.page_to + this.page_size;
    this.page_to = this.page_from - 1;
    this.page_from = this.page_to - this.page_size + 1;

    if ( this.page_from < 1 )
    {
      // this needs to be computed at the end of the range when the size is not equal with the standard size
      this.page_from = 1;
      this.chevUiService.dg_page_size = this.page_to - this.page_from + 1;
    } else {
      this.chevUiService.dg_page_size = this.page_size;
    }

    this.chevUiService.dg_db_name = this.db_name;
    this.chevUiService.dg_page_from = this.page_from;
    this.chevUiService.dg_page_to = this.page_to;

    this.chevUiService.triggerDgSearch();
  }


  onTest()
  {
/*
    this.chevUiService.testHttp().subscribe( (ret_data: DcThold[]) =>
      {
           console.log('DataClass list length: ' + ret_data.length.toString());
           console.log(ret_data);
      });
*/
      this.chevUiService.httpGetDcTholds();
  }


}
