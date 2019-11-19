
import { Injectable, OnInit, OnDestroy } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { of } from 'rxjs/observable/of';
import { Subject } from 'rxjs/Subject';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { HttpParams } from '@angular/common/http';
import { catchError, map, tap } from 'rxjs/operators';

import { URLSearchParams } from '@angular/http';

import { Table } from './table';
import { SearchCnt } from './table';
import { TABLELIST, SEARCHCNT } from './mock-ifsentity';

import { Ifsentity } from './ifsentity';
import { IFSENTITIES } from './mock-ifsentity';
import { EZENTITIES } from './mock-ifsentity';

import { Column } from './column';

import { Entitycol } from './entitycol';
import { VEHALLIEDINV, COLUMNLIST } from './mock-entitycol';
import { EZENTITYCOLS } from './mock-entitycol';

import { ColReg } from './colregexp';
import { COLREGLIST } from './mock-colregexp';

import { Colregexp } from './colregexp';
import { FINCAPRVMSTR } from './mock-colregexp';

import { Profileresult } from './profileresult';
import { MOCKPROFRESULT } from './mock-profresult';

import { RegExp } from './regexp';
import { REGEXPLISTENC } from './mock-regexp';

import { Regexp } from './regexp';
import { ENCREGEXP } from './mock-regexp';

import { SAMPLEDATA } from './mock-sampledata';
import { SAMPLECOLUMNS } from './mock-sampledata';

import { VALREGEXP } from './mock-val-regexp';


@Injectable()
export class SelectionService implements OnInit, OnDestroy {

  // public sel_meta_source: string; // to be deleted
  public cur_gdap_layer: string;
  public sel_search_by: string;
  public sel_search_val: string;

  public cur_offset: number;
  public page_size: number;

  // public cur_table: Table;

  public sel_ifs: string;
  public sel_entity: string;
  public sel_hv_schema: string;
  public sel_ent_schema: string;
  // public sel_column: string;

  public sel_table: Table;
  public sel_column: Column;
  public sel_regexp: RegExp;
  public sel_colreg: ColReg;
  public sel_table_id: string;
  public sel_column_id: string;
  public sel_colreg_id: string;
  public sel_regexp_id: string;

  public sel_col_id: string;
  public sel_col_name: string;
  public sel_col_type: string;
  public sel_col_size: string;
  
  public sel_regexp_name: string;
  public sel_regexp_def: string;
  public sel_col_regexp_desc: string; // column to regexp association description
  public sel_dt_cq_id: string;

  public search_trigger = 0;
  public reset_trigger = 0;
  public regexp_list_trigger = 0;
  public sample_data_trig = 0;
  public validate_regexp_trig = 0;
  public refresh_colregexp_trig = 0;

  public sel_server: string;
  public sel_no_of_records: string;

  public CHEVELLE_ENV: string;

  private newSearchTrigger = new Subject<number>();  // click on the search button of the search-init
  newSearch$ = this.newSearchTrigger.asObservable();

  private currentEntitySource = new Subject<string>();  // if the entity source is IC or edge-base
  currentEntity$ = this.currentEntitySource.asObservable();

  private currentTableSource = new Subject<string>();  // the table ID of the newly selected table
  currentTable$ = this.currentTableSource.asObservable();

  private currentColumnSource = new Subject<string>();  // click on the entitycol - the current column id
  currentColumn$ = this.currentColumnSource.asObservable();

  private resetEventTrigger = new Subject<number>();  // reset event for whoever whats to subscribe
  resetTrigger$ = this.resetEventTrigger.asObservable();

  private loadRegexpListEvent = new Subject<number>(); // event on loading the full regexp-list
  loadRegexpList$ = this.loadRegexpListEvent.asObservable();

  private currentRegexpSource = new Subject<string>(); // click on a new regexp on the regexp-list
  currentRegexp$ = this.currentRegexpSource.asObservable();

  private loadSampleDataEvent = new Subject<number>(); // click on get sample data
  loadSampleData$ = this.loadSampleDataEvent.asObservable();

  private runValidateRegexpEvent = new Subject<number>(); // click to validate a regular expression
  runValidateRegexp$ = this.runValidateRegexpEvent.asObservable();

  private currentColRegSource = new Subject<string>();
  currentColReg$ = this.currentColRegSource.asObservable();

  private refreshColRegEvent = new Subject<number>();
  refreshColReg$ = this.refreshColRegEvent.asObservable();

  private searchIC_url = '/search_ic/SearchIC';
  private searchEZ_url = '/search_ic/GetEZMeta';
  private getColDefnIC_url = '/search_ic/GetColDefnIC';
  private getEZColDefn_url = '/search_ic/GetEZColDefn';
  private getMaxisCfg_url = '/search_ic/GetMaxisCfg';

  private getValidateRegex_url = '/ValidateRegex';
  private getSampleData_url = '/GetSampleData';

  private getMaxisTables_url = '/GetMaxisTables';
  private getMaxisColumns_url = '/GetMaxisColumns';
  private getMaxisColReg_url = '/GetMaxisColReg';
  private getMaxisProfile_url = '/GetMaxisProfile';
  private runAssocColReg_url = '/AssociateColReg';
  private getSearchMaxCnt_url = '/GetSearchMaxCnt';
  private getChevelleEnvClient_url = '/ChevelleEnvClient';

  http_params: HttpParams;
  searchIC_params: HttpParams;
  searchEZ_params: HttpParams;
  searchTable_params: HttpParams;
  getColumns_params: HttpParams;
  getColReg_params: HttpParams;

  

  // urlSearchParams: URLSearchParams;

  constructor( private http_client: HttpClient ) {
    // alert('The SelectionService constructing now!'); // constructed on first load in the browser
   }

  ngOnInit() {
    this.search_trigger = 0;
    this.reset_trigger = 0;
    this.regexp_list_trigger = 0;
    this.sample_data_trig = 0;
    this.sel_table = { "Id": "", "TableName": "", "BusinessName": "", "SourceTableName": "", "Description": "", "Asms": "", "GdapArchLayer": "", "SourcingProject": "", "SubjectArea": "", "SourceDbName": "", "SourceSchemaName": "", "HiveSchema": "", "Location": "", "AutosysJobId": "", "CreatedBy": "", "CreatedDate": "", "UpdatedBy": "", "UpdatedDate": "", "ImportId": "", "TotalRecords": "", "TotalRecordsUpdatedDate": "" }; 
    this.sel_server = 'P1';
    this.sel_no_of_records = '20';

    this.cur_offset = 0;
    this.page_size = 200;

    // alert('The SelectionService initializing now!');
  }

  ngOnDestroy(){}

  getBackendEnvironment(): Observable<any>
  {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('format', 'json');
    return this.http_client.post<any>(this.getChevelleEnvClient_url, this.http_params)
    .pipe(
      catchError(this.handleError('runAssociateColReg', []))
    );
  }

  getBackendEnvironment_local(): Observable<any>
  {
    return of({ 'CHEVELLE_ENV': 'DEV' });
  }

  triggerNewSearch() {
    this.search_trigger++;
    this.newSearchTrigger.next(this.search_trigger);
  }

  triggerNewReset() {
    this.reset_trigger++;
    this.resetEventTrigger.next(this.reset_trigger);
  }

  triggerRegexpList() {
    // this.regexp_list_trigger++;
    this.loadRegexpListEvent.next(++this.regexp_list_trigger);
  }

  triggerSampleData() {
    this.sample_data_trig = this.sample_data_trig + 1;
    // alert('new sample data trigger: ' + this.sample_data_trig.toString());
    this.loadSampleDataEvent.next(this.sample_data_trig);
  }

  triggerValidateRegexp() {
    this.validate_regexp_trig++;
    this.runValidateRegexpEvent.next(this.validate_regexp_trig);
  }

  triggerRefreshColReg()
  {
    this.refresh_colregexp_trig++;
    this.refreshColRegEvent.next(this.refresh_colregexp_trig);
  }

  // Service message commands
  updateEntity(table: string) {
    this.currentEntitySource.next(table);
  }

  updateTable(table_id: string) {
    this.currentTableSource.next(table_id);
    // alert('Table ID is: ' + this.sel_table.Id);
  }

  updateColumn(column_id: string) {
    this.currentColumnSource.next(column_id);
    // alert('Column ID: ' + column_id);
  }

  updateRegexp(new_regexp: string)
  {
    this.currentRegexpSource.next(new_regexp);
  }

  updateColReg(new_colreg: string)
  {
    this.currentColRegSource.next(new_colreg);
  }
/*
  getIfsEntities_local(): Observable<Ifsentity[]> {
    return of(IFSENTITIES);
  }
*/
/*
  getIfsEntities(searchby: string, searchval: string): Observable<Ifsentity[]> {
    this.searchIC_params = new HttpParams();
    this.searchIC_params = this.searchIC_params.set('searchby', searchby);
    this.searchIC_params = this.searchIC_params.set('searchval', searchval);
    this.searchIC_params = this.searchIC_params.set('format', 'json');

    return this.http_client.get<Ifsentity[]>(this.searchIC_url, { params: this.searchIC_params} )
      .pipe(
        catchError(this.handleError('getIfsEntities', []))
      );
  }
*/
/*
  getEZEntities_local(): Observable<Ifsentity[]> {
    return of(EZENTITIES);
  }
*/
/*
  getEZEntities(searchby: string, searchval: string): Observable<Ifsentity[]> {
    this.searchEZ_params = new HttpParams();
    this.searchEZ_params = this.searchEZ_params.set('searchby', searchby);
    this.searchEZ_params = this.searchEZ_params.set('searchval', searchval);
    this.searchEZ_params = this.searchEZ_params.set('format', 'json');
    this.searchEZ_params = this.searchEZ_params.set('edge_type', 'edge-base');

    return this.http_client.get<Ifsentity[]>(this.searchEZ_url, { params: this.searchEZ_params} )
      .pipe(
        catchError(this.handleError('getEZEntities', []))
      );
  }
*/
/*
  getListEntities(): Observable<Ifsentity[]> 
  {
    if (this.cur_gdap_layer === 'IC')
    {
      this.searchIC_params = new HttpParams();
      this.searchIC_params = this.searchIC_params.set('searchby', this.sel_search_by);
      this.searchIC_params = this.searchIC_params.set('searchval', this.sel_search_val);
      this.searchIC_params = this.searchIC_params.set('format', 'json');

      return this.http_client.get<Ifsentity[]>(this.searchIC_url, { params: this.searchIC_params} )
        .pipe(
          catchError(this.handleError('getICEntities', []))
        );
    } else if (this.cur_gdap_layer === 'EDGE_BASE')
    {
      this.searchEZ_params = new HttpParams();
      this.searchEZ_params = this.searchEZ_params.set('searchby', this.sel_search_by);
      this.searchEZ_params = this.searchEZ_params.set('searchval', this.sel_search_val);
      this.searchEZ_params = this.searchEZ_params.set('format', 'json');
      this.searchEZ_params = this.searchEZ_params.set('edge_type', 'edge-base');

      return this.http_client.get<Ifsentity[]>(this.searchEZ_url, { params: this.searchEZ_params} )
        .pipe(
          catchError(this.handleError('getEZEntities', []))
        );
    } else
    {
      return of([]);
    }

  }
*/
/*
  getListEntities_local(): Observable<Ifsentity[]> 
  {
    if (this.cur_gdap_layer === 'IC')
    {
      return of(IFSENTITIES);
    } else if (this.cur_gdap_layer === 'EDGE_BASE')
    {
      return of(EZENTITIES);
    } else
    {
      return of([]);
    }
  }
*/

  getListTables_local(): Observable<Table[]>
  {
    return of(TABLELIST);
  }

  getListTables(): Observable<Table[]>
  {
    this.searchTable_params = new HttpParams();
    this.searchTable_params = this.searchTable_params.set('gdap_layer', this.cur_gdap_layer);
    this.searchTable_params = this.searchTable_params.set('searchby', this.sel_search_by);
    this.searchTable_params = this.searchTable_params.set('searchval', this.sel_search_val);
    this.searchTable_params = this.searchTable_params.set('offset', this.cur_offset.toString());
    this.searchTable_params = this.searchTable_params.set('page_size', this.page_size.toString());
    this.searchTable_params = this.searchTable_params.set('format', 'json');

    return this.http_client.get<Table[]>(this.getMaxisTables_url, { params: this.searchTable_params} )
      .pipe(
        catchError(this.handleError('getMaxisTables', []))
      );
  }

  getSearchCnt_local(): Observable<SearchCnt>
  {
    return of(SEARCHCNT);
  }

  getSearchCnt(): Observable<SearchCnt>
  {
    this.searchTable_params = new HttpParams();
    this.searchTable_params = this.searchTable_params.set('gdap_layer', this.cur_gdap_layer);
    this.searchTable_params = this.searchTable_params.set('searchby', this.sel_search_by);
    this.searchTable_params = this.searchTable_params.set('searchval', this.sel_search_val);
    this.searchTable_params = this.searchTable_params.set('format', 'json');

    return this.http_client.get<SearchCnt>(this.getSearchMaxCnt_url, { params: this.searchTable_params} );
  }

  getListColumns_local(): Observable<Column[]>
  {
    return of(COLUMNLIST);
  }


  getListColumns(): Observable<Column[]>
  {
    this.getColumns_params = new HttpParams();
    this.getColumns_params = this.getColumns_params.set('TableId', this.sel_table_id);
    this.getColumns_params = this.getColumns_params.set('format', 'json');

    return this.http_client.get<Column[]>(this.getMaxisColumns_url, { params: this.getColumns_params} )
      .pipe(
        catchError(this.handleError('getMaxisColumns', []))
      );
  }

  getListColReg_local(): Observable<ColReg[]>
  {
    this.decriptColRegList(COLREGLIST);
    return of(COLREGLIST);
  }

  getListColReg(): Observable<ColReg[]>
  {
    this.getColReg_params = new HttpParams();
    this.getColReg_params = this.getColReg_params.set('TableId', this.sel_table_id);
    this.getColReg_params = this.getColReg_params.set('format', 'json');

    return this.http_client.get<ColReg[]>(this.getMaxisColReg_url, { params: this.getColReg_params} )
      .pipe(
        catchError(this.handleError('getMaxisColReg', []))
      );

    //this.decriptColRegList(COLREGLIST);
  
  }


  getEntityCols_local(): Observable<Entitycol[]>  // testing function, covers both, the IC and EZ
  {
    if (this.cur_gdap_layer === 'IC')
    {
      return of(VEHALLIEDINV);
    } else if (this.cur_gdap_layer === 'EDGE_BASE')
    {
      return of(EZENTITYCOLS);
    } else {
      return of(VEHALLIEDINV);
    }

  }

  getEntityCols(cur_ifs: string, cur_entity: string, cur_hv_schema: string): Observable<Entitycol[]> {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('ifs', cur_ifs);
    this.http_params = this.http_params.set('entity', cur_entity);
    this.http_params = this.http_params.set('hv_schema', cur_hv_schema);
    this.http_params = this.http_params.set('format', 'json');

    // alert('getting the ColDefn for ifs: ' + cur_ifs + ' , entity: ' + cur_entity + ' , hv_schema: ' + cur_hv_schema);
    return this.http_client.get<Entitycol[]>(this.getColDefnIC_url, { params: this.http_params} )
      .pipe(
        catchError(this.handleError('getEntityCols', []))
      );
  }

  getEZEntityCols(): Observable<Entitycol[]> 
  {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('edge_type', 'edge-base');
    this.http_params = this.http_params.set('entity', this.sel_entity);
    this.http_params = this.http_params.set('ent_schema', this.sel_ent_schema);
    this.http_params = this.http_params.set('format', 'json');

    // alert('getting the ColDefn for ifs: ' + cur_ifs + ' , entity: ' + cur_entity + ' , hv_schema: ' + cur_hv_schema);
    return this.http_client.get<Entitycol[]>(this.getEZColDefn_url, { params: this.http_params} )
      .pipe(
        catchError(this.handleError('getEZEntityCols', []))
      );
  }
/*
  getColRegexpAssoc_local(): Observable<Colregexp[]> {
    return of(FINCAPRVMSTR);
  }
*/
/*
  getColRegexpAssoc(cur_hv_schema: string, cur_table: string): Observable<Colregexp[]> {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('get_what', '2');
    this.http_params = this.http_params.set('hv_schema', cur_hv_schema);
    this.http_params = this.http_params.set('table', cur_table);
    this.http_params = this.http_params.set('env', 'DEV');

    return this.http_client.get<Colregexp[]>(this.getMaxisCfg_url, { params: this.http_params } )
    .pipe(
      catchError(this.handleError('getColRegexpAssoc', []))
    );
  }
*/

  getProfileResult_local(): Observable<Profileresult> {
    return of(MOCKPROFRESULT);
  }

  getProfileresult(cur_hv_schema: string, cur_table: string, cur_col_name: string, cur_regexp_id: string): Observable<Profileresult> {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('get_what', '5');
    this.http_params = this.http_params.set('hv_schema', cur_hv_schema);
    this.http_params = this.http_params.set('table', cur_table);
    this.http_params = this.http_params.set('col_name', cur_col_name);
    this.http_params = this.http_params.set('regexp_id', cur_regexp_id);
    this.http_params = this.http_params.set('env', 'DEV');

    return this.http_client.get<Profileresult>(this.getMaxisCfg_url, { params: this.http_params});
    // need to catch the http error here

    // return this.http_client.get<Profileresult[]>(this.getMaxisCfg_url, { params: this.http_params } )
    // .pipe( catchError(this.handleError('getColRegexpAssoc', [])));
  }

  getProfileResult(colreg_id: string): Observable<Profileresult> 
  {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('ColRegId', colreg_id);
    this.http_params = this.http_params.set('format', 'json');

    return this.http_client.get<Profileresult>(this.getMaxisProfile_url, { params: this.http_params} );


  }

  runValidateRegexp(sample_text: string, sample_regex: string, sel_server: string ): Observable<any[]>
  {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('sample_text', sample_text);
    this.http_params = this.http_params.set('regexp', sample_regex);
    this.http_params = this.http_params.set('server', sel_server);
    this.http_params = this.http_params.set('format', 'json');

    return this.http_client.post<any[]>(this.getValidateRegex_url, this.http_params) // { params: this.http_params}
    .pipe(
      catchError(this.handleError('runValidateRegexp', []))
    );

  }

  runAssociateColReg(col_id: string, reg_id: string, assoc_name: string): Observable<any>
  {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('ColumnId', col_id);
    this.http_params = this.http_params.set('RegexpId', reg_id);
    this.http_params = this.http_params.set('ColRegName', assoc_name);
    this.http_params = this.http_params.set('format', 'json');

    return this.http_client.post<any>(this.runAssocColReg_url, this.http_params) 
    .pipe(
      catchError(this.handleError('runAssociateColReg', []))
    );

  }

  runAssociateColReg_local(col_id: string, reg_id: string, assoc_name: string): Observable<any>
  {
    return of({ 'stat': 'OK' });
  }

/*
  runValidateRegexp(param_text: string, param_regex: string, param_server: string ): Observable<any[]>
  {
    let urlSearchParams = new URLSearchParams();

    urlSearchParams.append('sample_text', param_text);
    urlSearchParams.append('regexp', param_regex);
    urlSearchParams.append('server', param_server);
    urlSearchParams.append('format', 'json');

    return this.http_client.post<any[]>(this.getValidateRegex_url, urlSearchParams)
      .pipe(
        catchError(this.handleError('runValidateRegexp', []))
    );

  }
*/

/*
  getSampleData(param_server: string, param_path: string, param_records: string): Observable<any[][]>
  {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('server', param_server);
    this.http_params = this.http_params.set('hdfs_path', param_path);
    this.http_params = this.http_params.set('no_of_records', param_records);
    this.http_params = this.http_params.set('format', 'json');

    return this.http_client.get<any[][]>(this.getSampleData_url, { params: this.http_params } )
      .pipe(
        catchError(this.handleError('getColRegexpAssoc', []))
      );
  }
*/

  getSampleData(): Observable<any[][]>
  {
    this.http_params = new HttpParams();
    this.http_params = this.http_params.set('server', this.sel_server);
    this.http_params = this.http_params.set('hdfs_path', this.sel_table.Location);
    this.http_params = this.http_params.set('no_of_records', this.sel_no_of_records);
    this.http_params = this.http_params.set('format', 'json');

    return this.http_client.get<any[][]>(this.getSampleData_url, { params: this.http_params } )
      .pipe(
        catchError(this.handleError('getColRegexpAssoc', []))
      );
  }



  getRegexpList_local(): Observable<RegExp[]>
  {
    // this.decriptRegexpList(ENCREGEXP);
    // return of(ENCREGEXP);
    return of(REGEXPLISTENC);
  }

  getSampleData_local(): Observable<any[][]>
  {
    return of(SAMPLEDATA);
  }

  getSampleColumns_local(): Observable<any[]>
  {
    return of(SAMPLECOLUMNS);
  }

  runValidateRegexp_local(): Observable<any[]>
  {
    return of(VALREGEXP);
  }

  getColRegDetails_local(cur_colreg_id: string): Observable<ColReg>
  {
    return of(this.sel_colreg);
  }

  getColRegDetails(cur_colreg_id: string): Observable<ColReg>
  {
    return of(this.sel_colreg);
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

  decriptRegexpList(enc_regexp_list: Regexp[]): void
  {
    let i: number;
    for (i = 0; i < enc_regexp_list.length; i++)
    {
      enc_regexp_list[i].regexp = this.decriptRegexp(enc_regexp_list[i].regexp);
    }
  }

  

  decriptColRegList(enc_colreg_list: ColReg[])
  {
    let i: number;
    for (i = 0; i < enc_colreg_list.length; i++)
    {
      enc_colreg_list[i].Regexp = this.decriptRegexp(enc_colreg_list[i].Regexp);
      // alert(enc_colreg_list[i].Regexp);
    }
  }

  private handleHttpError() {
    return (error: any): void => {
      console.error(error); // log to console instead
      console.log(error.message);
      alert('HTTP call failed: ' + error.message);
    }
  }

/*
 * Handle Http operation that failed.
 * Let the app continue.
 * @param operation - name of the operation that failed
 * @param result - optional value to return as the observable result
 */

private handleError<T> (operation = 'operation', result?: T) {
  return (error: any): Observable<T> => {

    // TODO: send the error to remote logging infrastructure
    console.error(error); // log to console instead
    console.log(error.message);

    // TODO: better job of transforming error for user consumption
    // this.log(`${operation} failed: ${error.message}`);
    alert('HTTP call failed: ' + error.message);

    // Let the app keep running by returning an empty result.
    return of(result as T);
  };
}


}

