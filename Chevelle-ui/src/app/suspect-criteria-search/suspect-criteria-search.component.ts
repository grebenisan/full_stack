import { Component, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';
import { ChevUiService } from '../chev-ui.service';

import { DcName } from '../dataclass';
import { SuspectCriteria, SUSPECT_CRITERIA_HEADERS, SUSPECT_CRITERIA_EMPTY, SUSPECT_CRITERIA_DATA_EMPTY } from '../suspect-criteria';

// import { AuthService } from '../auth.service'; // just for testing
// export class DcName {
//   dc_name: string;
//  dc_val: string;
// }

@Component({
  selector: 'app-suspect-criteria-search',
  templateUrl: './suspect-criteria-search.component.html',
  styleUrls: ['./suspect-criteria-search.component.css']
})
export class SuspectCriteriaSearchComponent implements OnInit {

  sc_data_subscription: Subscription;   // suspect criteria data is available (has returned) from the microservice
  running: boolean;
  dc_names: DcName[];
  sel_dc_name: string;
  filter_by: string;

  // sc_search_disabled: boolean;
 
  constructor( private chevUiService: ChevUiService  ) { } // ,private authService: AuthService

  ngOnInit() {
    //this.sc_search_disabled = false;
    this.running = false;
   
    this.chevUiService.httpDcNameList();
    this.dc_names = this.chevUiService.dc_name_list;
    // console.log(this.dc_names);

/*    this.dc_names = [
      {dc_name: 'ACCOUNT_NUMBER', dc_val: 'ACCOUNT_NUMBER' },
      {dc_name: 'BANK_NAME', dc_val: 'BANK_NAME' },
      {dc_name: 'BRITISH_NINO', dc_val: 'BRITISH_NINO' },
      {dc_name: 'CREDIT_REPORT_DATA', dc_val: 'CREDIT_REPORT_DATA' },
      {dc_name: 'DRIVERS_LICENSE_NUMBER', dc_val: 'DRIVERS_LICENSE_NUMBER' },
      {dc_name: 'EMAIL_ADDRESS', dc_val: 'EMAIL_ADDRESS' },
      {dc_name: 'EXPIRATION_DATE', dc_val: 'EXPIRATION_DATE' },
      {dc_name: 'FRENCH_INSEE', dc_val: 'FRENCH_INSEE' },
      {dc_name: 'LATITUDE', dc_val: 'LATITUDE' },
      {dc_name: 'LOGIN_ID', dc_val: 'LOGIN_ID' },
      {dc_name: 'LONGITUDE', dc_val: 'LONGITUDE' },
      {dc_name: 'NAME', dc_val: 'NAME' },
      {dc_name: 'PHONE_NUMBER', dc_val: 'PHONE_NUMBER' },
      {dc_name: 'PIN_NUMBER', dc_val: 'PIN_NUMBER' },
      {dc_name: 'ROUTING_NUMBER', dc_val: 'ROUTING_NUMBER' },
      {dc_name: 'SERVICE_CODE', dc_val: 'SERVICE_CODE' },
      {dc_name: 'STREET_ADDRESS', dc_val: 'STREET_ADDRESS' },
    ];
*/
    this.sel_dc_name = 'ACCOUNT_NUMBER';
    this.filter_by = 'all';


    this.sc_data_subscription = this.chevUiService.obsvScInformSearch$.subscribe( sc_inform_ret => {

        // alert('Suspect Criteria Search: got a new SC data avilable trigger: ' + sc_inform_ret.toString())
        this.running = false;
      });

  }

  onTest()
  {

    // this.authService.authInit();

    // this.chevUiService.httpDcNameList();
    // this.dc_names = this.chevUiService.dc_name_list;

    // this.chevUiService.getAuth();
    // window.open('https://servicesso.domain.com/jwttoken');
    // window.URL = 'https://servicesso.domain.com/jwttoken'
    
    // this.chevUiService.login_win = window.open('https://servicesso.domain.com/jwttoken', '_blank', 'fullscreen=no,toolbar=yes,scrollbars=yes,resizable=yes,top=200,left=500,width=400,height=400');

    // this.chevUiService.login_win.addEventListener('loadend', this.chevUiService.loginWindowLoadend() );
    // this.chevUiService.login_win.addEventListener('loadeddata', this.chevUiService.loginWindowLoadedData() );
    // this.chevUiService.login_win.addEventListener('loadend', this.chevUiService.loginWindowLoadEnd() );
    // this.chevUiService.login_win.addEventListener('pageshow', this.chevUiService.loginWindowPageShow() );
    // this.chevUiService.login_win.addEventListener('loadend', this.chevUiService.loginWindowLoadEnd() );
    // this.chevUiService.login_win.addEventListener('loadeddata', this.chevUiService.loginWindowLoadedData() );
    // this.chevUiService.login_win.addEventListener('loadend', this.chevUiService.loginWindowLoadend() );

    // this.chevUiService.login_win.focus();

    // console.log('inner Text: ' + this.chevUiService.login_win.document.documentElement.innerText);
    // this.chevUiService.login_win = window.open('https://servicesso.domain.com/jwttoken', '_blank', 'fullscreen=no,toolbar=yes,scrollbars=yes,resizable=yes,top=200,left=500,width=400,height=400');
 

   //  this.chevUiService.login_win.focus();
    // window.addEventListener('loadeddata')
    //this.chevUiService.login_win
    //let evt: Event;
    //let tok = newWindow.onloadeddata(evt);
    //newWindow.close();
    //window.document.addEventListener('loadend'
    //window.document.documentElement.nodeValue
    // this.chevUiService.login_win.print();
   // window.addEventListener('pageshow')
  }

  onFilterBy(filter_by: string)
  {
    if ( filter_by === 'all' )
    {
      this.filter_by = 'all';
    }
    if ( filter_by === 'data_class' )
    {
      this.filter_by = 'data_class';
      // this.chevUiService.httpDcNameList();
      this.dc_names = this.chevUiService.dc_name_list;
    }

  }

  isDisabled(): boolean {
    if ( this.filter_by === 'all' )
    {
      return true;
    } else if ( this.filter_by === 'data_class' )
    {
      return false;
    } else
    {
      return false;
    }
  }

  onFilterSuspectCriteria()
  {
    //alert('Filter suspect criteria!');
    this.running = true;
    this.chevUiService.sc_list = SUSPECT_CRITERIA_DATA_EMPTY;
    this.chevUiService.sel_sc_rec = SUSPECT_CRITERIA_EMPTY;

    // time out only for demo
//    setTimeout(() => {
//      this.chevUiService.triggerListSuspectCriteria();
//    }, 1000);

     this.chevUiService.triggerListSuspectCriteria(this.filter_by, this.sel_dc_name);
  }

  onSelDcNameFilter(new_dc_name)
  {
    // alert('New DC name filter: ' + new_dc_name);
    this.sel_dc_name = new_dc_name;
  }
}
