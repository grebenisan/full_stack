import { Injectable, OnInit, OnDestroy } from '@angular/core';
import { Observable } from 'rxjs';
import { of } from 'rxjs';
import { Subject } from 'rxjs';
import { environment } from '../environments/environment';


import { HttpClient, HttpHeaders, HttpResponse } from '@angular/common/http';
import { HttpParams } from '@angular/common/http';
import { catchError, map, tap } from 'rxjs/operators';
import { URLSearchParams, RequestOptions } from '@angular/http';

import { DgSensitive, DgUpdateCol } from './dg-sensitive';
import { DG_SENSI_DATA } from './mock-dg-sensi-list';

import { SuspectCriteria } from './suspect-criteria';
import { SUSPECT_CRITERIA_DATA } from './mock-suspect-criteria';

import { DcThold, DcName } from './dataclass';

import { User } from './user';
import { stringify } from '@angular/core/src/util';
import { Window } from 'selenium-webdriver';

@Injectable({
  providedIn: 'root'
})
export class ChevUiService implements OnInit, OnDestroy {

  public running_mode = 'cloud';
  public sel_dg_col: DgSensitive; // the currently selected data governance column

  public dc_thold_list: DcThold[];
  public dc_name_list: DcName[];

  public dg_total_cnt: number;  // to be removed
  public dg_total: number;

  public dg_pending_cnt: number; // to be removed

  public dg_pending_page: number;
  public dg_pending_total: number;
  public dg_pending_over: number;

  public dg_pending_str: string; // to be removed

  public dg_pending_page_msg: string;
  public dg_pending_total_msg: string;

  public sel_db_name: string; // to be removed
  public dg_db_name: string;
  public sel_page_size: number; // to be removed
  public sel_page_offset: number; // to be removed

  public dg_page_from: number;
  public dg_page_to: number;
  public dg_page_size: number;

  public sc_list: SuspectCriteria[];
  public sel_sc_rec: SuspectCriteria; // the currently selected suspect criteria record

  private dg_search_trigger = 0;
  private subjDgSearch =  new Subject<number>();
  obsvDgSearch$ = this.subjDgSearch.asObservable();

  private dg_col_sel_trigger = 0;
  private subjDgColSelect = new Subject<number>();
  obsvDgColSelect$ = this.subjDgColSelect.asObservable();

  private dg_col_upd_trigger = 0;
  private subjDgColUpdate = new Subject<string>();
  obsvDgColUpdate$ = this.subjDgColUpdate.asObservable();

  private dg_inform_search_trigger = 0;
  private subjDgInformSearch = new Subject<number>();
  obsvDgInformSearch$ = this.subjDgInformSearch.asObservable();

  private sc_inform_search_trigger = 0;  // trigger to inform other components about data for suspect criteria
  private subjScInformSearch = new Subject<number>();
  obsvScInformSearch$ = this.subjScInformSearch.asObservable(); // inform other components the suspect criteria data is available

  private sc_inform_selection_trigger = 0; // trigger to inform other components about a new suspect criteria selection
  private subjScInformSelection = new Subject<number>();
  obsvScInformSelection$ = this.subjScInformSelection.asObservable(); // inform other components  there is a new suspect criteria seleted

  private sc_inform_update_trigger = 0;  // trigger to inform other components about data for suspect criteria
  private subjScInformUpdate = new Subject<string>();
  obsvScInformUpdate$ = this.subjScInformUpdate.asObservable(); // inform other components the suspect criteria data has updated or changed

  private micro_base_url: string;

  private get_HttpTest_url = 'https://chevelle-ui-micro-dev.server.domain.com/suspectcriteria/test';
  private url_dg_getreview = 'https://chevelle-ui-micro-dev.server.domain.com/datagov/getreview';
  // private url_dg_getreview = 'http://localhost:8090/datagov/getreview';
  private url_dg_updatecol = 'https://chevelle-ui-micro-dev.server.domain.com/datagov/updatecol';
  private url_dg_updatebatch = 'https://chevelle-ui-micro-dev.server.domain.com/datagov/updatebatch';
  private url_dataclass_tholds = 'https://chevelle-ui-micro-dev.server.domain.com/regex/gettholds';

  private url_suspect_criteria_all = 'https://chevelle-ui-micro-dev.server.domain.com/suspectcriteria/getall';
  private url_suspect_criteria_by_dc = 'https://chevelle-ui-micro-dev.server.domain.com/suspectcriteria/get/';
  private url_suspect_criteria_update = 'https://chevelle-ui-micro-dev.server.domain.com/suspectcriteria/save';
  private url_suspect_criteria_delete = 'https://chevelle-ui-micro-dev.server.domain.com/suspectcriteria/putdelete';

  private url_authenticate = 'https://servicesso.cpi.domain.com/jwttoken';

  public login_win: any;


  constructor( private http_client: HttpClient ) {
    console.log(' ChevUiService : constructor');
   }

  ngOnInit() {

    this.running_mode = 'cloud';

    if (environment.env === 'dev')
    {
      this.micro_base_url = 'https://chevelle-ui-micro-dev.server.domain.com';
    } else
    if (environment.env === 'development')
    {
      this.micro_base_url = 'https://chevelle-ui-micro-dev.server.domain.com';
    } else
    if (environment.env === 'test')
    {
      this.micro_base_url = 'https://chevelle-ui-micro-test.server.domain.com';
    } else
    if (environment.env === 'qa')
    {
      this.micro_base_url = 'https://chevelle-ui-micro-qa.server.domain.com';
    } else
    if (environment.env === 'prod')
    {
      this.micro_base_url = 'https://chevelle-ui-micro.cpi.domain.com';
    } else {
      this.micro_base_url = 'https://chevelle-ui-micro-dev.server.domain.com';
    }



    this.sc_list = null;
    this.sel_sc_rec = null;

    this.dc_thold_list = null;
    this.dc_name_list = null;

    this.dg_search_trigger = 0;
    this.dg_col_sel_trigger = 0;
    this.dg_col_upd_trigger = 0;
    this.dg_inform_search_trigger = 0;
    this.sc_inform_search_trigger = 0;
    this.sc_inform_selection_trigger = 0;
    this.sel_dg_col = null;

    this.dg_total_cnt = null; // to be removed
    this.dg_total = 0;

    this.dg_pending_cnt = 0; // to be removed
    this.dg_pending_page = 0;
    this.dg_pending_total = 0;
    this.dg_pending_over = 0;

    this.dg_pending_str = 'Search a database for columns to be reviewed!'; // to be removed
    this.dg_pending_page_msg = 'Search a database for columns to be reviewed!';
    this.dg_pending_total_msg = 'Search a database for columns to be reviewed!';

    this.sel_db_name = '';  // to be removed
    this.dg_db_name = '';
    this.sel_page_size = 10;  // to be removed
    this.sel_page_offset = 1; // to be removed
    this.dg_page_from = 1;
    this.dg_page_to = 10;
    this.dg_page_size = 10;


    this.get_HttpTest_url = this.micro_base_url + '/suspectcriteria/test';
    this.url_dg_getreview = this.micro_base_url + '/datagov/getreview';
    // this.url_dg_getreview = 'http://localhost:8090/datagov/getreview';
    this.url_dg_updatecol = this.micro_base_url + '/datagov/updatecol';
    this.url_dg_updatebatch = this.micro_base_url + '/datagov/updatebatch';
    this.url_dataclass_tholds = this.micro_base_url + '/regex/gettholds';
    this.url_suspect_criteria_all = this.micro_base_url + '/suspectcriteria/getall';
    this.url_suspect_criteria_by_dc = this.micro_base_url + '/suspectcriteria/get/';
    this.url_suspect_criteria_update = this.micro_base_url + '/suspectcriteria/save';
    this.url_suspect_criteria_delete = this.micro_base_url + '/suspectcriteria/putdelete';
    this.url_authenticate = 'https://servicesso.cpi.domain.com/jwttoken';

    this.login_win = null;
  }

  ngOnDestroy(){}

  triggerDgSearch() {

    this.dg_total_cnt = null; // to be removed
    this.dg_pending_cnt = null; // to be removed

    this.sel_dg_col = null;

    this.dg_total = null;
    this.dg_pending_page = null;
    this.dg_pending_total = null;
    this.dg_pending_over = null;

    // first make the http call and get the result

    // calculate current_offset, current_pagesize, total count

    this.dg_search_trigger++;
    this.subjDgSearch.next(this.dg_search_trigger);
  }

  getListDgSensitive(): Observable<any>
  {
    if ( this.running_mode === 'cloud' )
    {
      let dg_review_params = new HttpParams();
      let dg_review_headers = new HttpHeaders();
      // let dg_review_options = new Http

      // dg_review_params = dg_review_params.set('dbname', this.sel_db_name);
      // dg_review_params = dg_review_params.set('page_size', this.sel_page_size.toString());
      // dg_review_params = dg_review_params.set('page_offset', this.sel_page_offset.toString());

      dg_review_params = dg_review_params.append('dbname', this.sel_db_name);
      dg_review_params = dg_review_params.append('page_size', this.dg_page_size.toString());
      dg_review_params = dg_review_params.append('page_offset', this.dg_page_from.toString());

      dg_review_headers = dg_review_headers.append('Access-Control-Expose-Headers', 'etag');

      return this.http_client.get(this.url_dg_getreview,
        { params: dg_review_params , observe: 'response' , headers: dg_review_headers } )  // , headers: dg_review_headers
      .pipe(
        // tap(_ => alert('successfully tapped into and fetched Http Test')),
        catchError(this.handleError('getListDgSensitive', []))
      );
    }

    if (this.running_mode === 'local')
    {
      let i: number;
      this.dg_total_cnt = DG_SENSI_DATA.length;

      // decript the regexp field
      for (i = 0; i < this.dg_total_cnt; i++)
      {
        DG_SENSI_DATA[i].appl_regex_str = this.decriptRegexp(DG_SENSI_DATA[i].appl_regex_str);
      }

      return of(DG_SENSI_DATA);

    } 
  }


  httpDgReviewUpdateCol(upd_col: DgUpdateCol ): Observable<any>
  {

    // alert('upd_col: ' + upd_col);
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json'
      })
    };

    return this.http_client.put(this.url_dg_updatecol, upd_col, httpOptions)
    .pipe(
      // tap( data => { console.log(data); } ),
      catchError(this.handleError('httpDgReviewUpdateCol', upd_col))
    );

  }


  httpDgReviewUpdateBatch(upd_batch: DgUpdateCol[] ): Observable<any>
  {

    // alert('upd_col: ' + upd_col);
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json'
      })
    };

    return this.http_client.put(this.url_dg_updatebatch, upd_batch, httpOptions)
    .pipe(
      // tap( data => { console.log(data); } ),
      catchError(this.handleError('httpDgReviewUpdateBatch', upd_batch))
    );

  }


  httpGetDcTholds()
  {
    // make the http only if the call was NOT made previously
    if (this.dc_thold_list === null || this.dc_thold_list === undefined)
    {

      this.http_client.get<DcThold[]>(this.url_dataclass_tholds) // {responseType: 'text'}
        .pipe(
        catchError(this.handleError('testHttp', []))
        )
        .subscribe( ret_thold_list =>
        {
          this.dc_thold_list = ret_thold_list;
          this.triggerUpdateDgCol('recommend');
          // console.log(this.dc_thold_list);

          // console.log('Thold for BANK_NAME is: ' + this.getTholdByDcName('BANK_NAME').toString() );
          // create a map or a structure to return the thold base on dc_name
          // create the list of DC names

          // inform the other components about the dc names and tholds
        });
    } else {
      this.triggerUpdateDgCol('recommend');
    }
  }

  httpDcNameList()
  {
    if (this.dc_thold_list === null || this.dc_thold_list === undefined)
    {
      this.http_client.get<DcThold[]>(this.url_dataclass_tholds)
        .pipe(
        catchError(this.handleError('testHttp', []))
        )
        .subscribe( ret_thold_list =>
        {
          this.dc_thold_list = ret_thold_list;
          this.createDcNameList();
          // console.log(this.dc_name_list);
        });
    } else {
      if ( this.dc_name_list === null || this.dc_name_list === undefined )
      {
        this.createDcNameList();
        // console.log(this.dc_name_list); 
      }
    }
  }


  createDcNameList()
  {
    let i: number;
    let local_dc: DcName;
    this.dc_name_list = new Array<DcName>();

    for (i = 0; i < this.dc_thold_list.length; i++)
    {
      local_dc = new DcName();
      local_dc.dc_name = this.dc_thold_list[i].data_cls_nm;
      local_dc.dc_val = this.dc_thold_list[i].data_cls_nm;
      this.dc_name_list.push(local_dc);
    }
    // console.log(this.dc_name_list);
  }



  getTholdByDcName(dc_name_to_find: string): number {
    let found_thold = Number(50.0).valueOf();
    let i: number;
    if (this.dc_thold_list === null || this.dc_thold_list === undefined)
    {
      return found_thold;
    } else {
      for (i = 0; i < this.dc_thold_list.length; i++)
      {
        if (this.dc_thold_list[i].data_cls_nm === dc_name_to_find)
        {
          found_thold = this.dc_thold_list[i].match_thold_pct;
          return found_thold;

        }
      }
    }



    return found_thold;
  }


  testHttp(): Observable<DcThold[]>
  {

      // dbname=GPDHUB1P&page_size=100&page_offset=1
      // let test_params = new HttpParams();
      // let test_url = 'https://chevelle-ui-micro-dev.server.domain.com/suspectcriteria/test';
      // test_params = test_params.set('dbname', 'GPDHUB1P');
      // test_params = test_params.set('page_size', '100');
      // test_params = test_params.set('page_offset', '1');

      return this.http_client.get<DcThold[]>(this.url_dataclass_tholds) // {responseType: 'text'}
        .pipe(
          // tap(_ => alert('successfully tapped into and fetched Http Test')),
          catchError(this.handleError('testHttp', []))
        );

  }

  testHttpPUT(): Observable<any>
  {
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json'
        // 'Authorization': 'my-auth-token'
      })
    };

    let upd_col = new DgUpdateCol();

    upd_col.dg_status = '20';
    upd_col.data_cls_nm = 'DRIVERS_LICENSE_NUMBER';
    upd_col.col_id = 'BB3F72DCC8E01D72217F56B33B73863FCAFB3725';
    upd_col.table_id = '00E1B96738AB07FF38193603071F481E5F4B0B86';
    upd_col.crt_ts = '2018-09-05 18:24:43';
    upd_col.dg_upd_by = 'bzy91q';
    upd_col.batch_id = 369;

    return this.http_client.put(this.url_dg_updatecol, upd_col, httpOptions)
    .pipe(
      tap( data => {
          console.log(data);
        }),
      catchError(this.handleError('httpOptions', upd_col))
    );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
   
      // TODO: send the error to remote logging infrastructure
      console.error(error); // log to console instead
      console.log(error.message);
      // TODO: better job of transforming error for user consumption
      // this.log(`${operation} failed: ${error.message}`);
      alert('Http error: ' + error.message);
   
      // Let the app keep running by returning an empty result.
      return of(result as T);
    };
  }

  triggerSelectDgCol() {
    this.dg_col_sel_trigger++;
    this.subjDgColSelect.next(this.dg_col_sel_trigger);
  }

  triggerUpdateDgCol(dg_type: string) {
    this.dg_col_upd_trigger++;
    let temp_upd_str: string;
    temp_upd_str = this.dg_col_upd_trigger.toString() + '_' + dg_type;
    this.subjDgColUpdate.next(temp_upd_str);

    // alert('Service Update trigger: ' + temp_upd_str);
  }

  triggerInformSearch() {
    this.subjDgInformSearch.next(++this.dg_inform_search_trigger);
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


  encriptRegexp(dec_regexp: string): string
  {
    let i: number;
    let buff_string = '';
  
    const backslash_char = String.fromCharCode(92); // the backslash char "\" - ascii decimal 92
    const open_brace_char = String.fromCharCode(123); // opening brace char “{“ - ascii decimal  123
    const close_brace_char = String.fromCharCode(125);  // closing brace char "}" - ascii decimal  125

    const char_169 = String.fromCharCode(169);  // the copyright character "©" - ascii decimal 169
    const char_171 = String.fromCharCode(171);  // left double angle quotes character "«" - ascii decimal 171 
    const char_187 = String.fromCharCode(187);  // right double angle quotes char "»" - ascii decimal 187 

    for (i = 0; i < dec_regexp.length; i++)
    {
      if (dec_regexp.charAt(i) === backslash_char)
      {
        // enc_regexp = enc_regexp.replace(char_169, backslash_char);
        buff_string += char_169;
      } else
      if (dec_regexp.charAt(i) === open_brace_char)
      {
        // enc_regexp = enc_regexp.replace(char_171,  open_brace_char);
        buff_string += char_171;
      } else
      if (dec_regexp.charAt(i) === close_brace_char)
      {
        // enc_regexp = enc_regexp.replace(char_171,  open_brace_char);
        buff_string += char_187;
      } else
      {
        buff_string += dec_regexp.charAt(i);
      }
    }
    return buff_string;
  }

  triggerListSuspectCriteria(filter_by: string, sel_dc_name: string): void
  {
    // clear the list first
    this.sc_list = null;

    // the result of the http call (or local)
        // wait to get the result
    // assign the result to the sc_list
    this.getHttpListSuspectCriteria(filter_by, sel_dc_name).subscribe(
      ret_sc_list =>
      {
        this.sc_list = ret_sc_list;

        // now inform the other components
        this.informNewListSuspectCriteria();
      }
    );


  }

  getHttpListSuspectCriteria(filter_by: string, sel_dc_name: string): Observable<SuspectCriteria[]>
  {

    let current_url: string;

    if (filter_by === 'all')
    {
      current_url = this.url_suspect_criteria_all;
    }

    if (filter_by === 'data_class')
    {
      current_url = this.url_suspect_criteria_by_dc + sel_dc_name;
    }

//    if (this.running_mode === 'local')
//    {
//      return of(SUSPECT_CRITERIA_DATA);
//    }
    // make the http call 
    return this.http_client.get<SuspectCriteria[]>(current_url) // {responseType: 'text'}
        .pipe(
        // tap(_ => alert('successfully tapped into and fetched Http Test')),
        catchError(this.handleError('getHttpListSuspectCriteria', []))
    );

  }


  informNewListSuspectCriteria()
  {
    this.sc_inform_search_trigger++;
    this.subjScInformSearch.next(this.sc_inform_search_trigger);
  }

  informNewUpdateSuspectCriteria(event_type: string)
  {
    this.sc_inform_update_trigger++;
    this.subjScInformUpdate.next(this.sc_inform_update_trigger.toString() + '_' + event_type);
  }


  getListSuspectCriteria(): SuspectCriteria[]
  {
    // need a better cloning  here
      return [...this.sc_list];
  }

  triggerSuspectCriteriaSelection()
  {
    this.sc_inform_selection_trigger++;
    this.subjScInformSelection.next(this.sc_inform_selection_trigger);
  }

  // update the sc_list with new value at a certain index
  updateScListValue(upd_sc: SuspectCriteria): void
  {
    let local_sc_list = [...this.sc_list];
    let i: number;
    for (i = 0; i < local_sc_list.length; i++)
    {
      if ( local_sc_list[i].id === upd_sc.id )
      {
        local_sc_list[i] = this.cloneSuspectCriteria(upd_sc);
      }
    }

    this.sc_list = local_sc_list;
 
  }

  cloneSuspectCriteria(sc: SuspectCriteria): SuspectCriteria
  {
    let cloned_sc = new SuspectCriteria();

    cloned_sc.data_cls_nm = sc.data_cls_nm.valueOf();
    cloned_sc.match_type_opr = sc.match_type_opr.valueOf();
    cloned_sc.match_nm = sc.match_nm.valueOf();
    cloned_sc.upd_by = sc.upd_by;
    cloned_sc.upd_ts = sc.upd_ts;
    cloned_sc.upd_rsn_txt = sc.upd_rsn_txt;
    cloned_sc.id = sc.id;

    return cloned_sc;
  }

  deleteSuspectCriteria(del_sc: SuspectCriteria): void
  {
    let local_sc_list = [...this.sc_list];
    let i: number;
    for (i = 0; i < local_sc_list.length; i++)
    {
      if ( local_sc_list[i].id === del_sc.id )
      {
        local_sc_list.splice(i, 1);
      }
    }

    this.sc_list = local_sc_list;
     
  }

  addNewSuspectCriteria( new_sc: SuspectCriteria ): void
  {
    let local_sc_list = [...this.sc_list];
    // local_sc_list.push(this.cloneSuspectCriteria(new_sc));
    local_sc_list.unshift(this.cloneSuspectCriteria(new_sc));

    this.sc_list = local_sc_list;
  }

  findMaximRowIdNext(): number
  {
    // alert('Something');
    let mx: number;
    let i: number;
    let cur_id: number;

    mx = 0;
    i = 0;

    // alert('Array length: ' + this.sc_list.length);

    for (i = 0; i < this.sc_list.length; i++)
    {
      // alert('Cur row nr: ' + this.sc_list[i].row_nr);

      cur_id = Number(this.sc_list[i].id).valueOf() || 0;
      if ( cur_id > mx )
        { mx =  cur_id; }
    }

    // alert('Maxim row ID: ' + mx.toString());

    return (mx + 1).valueOf();
  }


  httpUpdateSuspectCriteria(upd_sc: SuspectCriteria ): Observable<any>
  {

    // alert('upd_col: ' + upd_col);
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json'
      })
    };

    return this.http_client.post(this.url_suspect_criteria_update, upd_sc, httpOptions)
    .pipe(
      // tap( data => { console.log(data); } ),
      catchError(this.handleError('httpUpdateSuspectCriteria', upd_sc))
    );

  }

  httpDeleteSuspectCriteria(delete_sc: SuspectCriteria ): Observable<any>
  {

    console.log('Delete this SuspectCriteria:');
    console.log(delete_sc);
    // alert('upd_col: ' + upd_col);
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json'
      })
    };
    // Angular limitation: cannot send data in the body of a message for DELETE commands
    return this.http_client.put(this.url_suspect_criteria_delete, delete_sc, httpOptions)
      .pipe(
      catchError(this.handleError('httpDeleteSuspectCriteria', delete_sc))
    );

  }

/*
  getAuth()
  {
    return this.http_client.get(this.url_authenticate, {responseType: 'text'}) // {responseType: 'text'}
        .pipe(
        // tap(_ => alert('successfully tapped into and fetched Http Test')),
        catchError(this.handleError('getHttpListSuspectCriteria', []))
    ).subscribe (ret_token => {
      console.log('JWT auth token: ' + ret_token.valueOf().toString());
    });

  }

  loginWindowLoadend()
  {
    console.log('Login window: loaded');
    //this.login_win.close();
  }

  loginWindowLoadedData()
  {
    console.log('Login window: loadeddata');
    //this.login_win.close();
    //console.log('contentType: ' + this.login_win.document.contentType);
    // this.login_win.document.write('This is my text written in the document');
    //console.log('nodeType' + this.login_win.document.documentElement.nodeType);
  }

  loginWindowLoadEnd()
  {
    console.log('Login window: loadend');
    // let buf = this.login_win.document.documentElement.innerText || this.login_win.document.documentElement.textContent;
    // let buf = this.login_win.document.innerText || this.login_win.document.textContent;
    //console.log('node value: ' + this.login_win.document.documentElement.innerHTML);
  }

  loginWindowPageShow()
  {
    console.log('Login window: pageshow');
    //console.log('node value: ' + this.login_win.document.documentElement.innerHTML);
  }
*/



}
