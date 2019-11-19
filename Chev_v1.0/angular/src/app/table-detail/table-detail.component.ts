import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';

import { Table } from '../table';


@Component({
  selector: 'app-table-detail',
  templateUrl: './table-detail.component.html',
  styleUrls: ['./table-detail.component.css']
})
export class TableDetailComponent implements OnInit, OnDestroy {

  cur_table: Table;
  cur_table_id: string;
  subscription: Subscription;

  constructor(private selService: SelectionService) { }

  ngOnInit() {
    // this.selService.sel_table.TableName;
    this.cur_table = { "Id": "", "TableName": "", "BusinessName": "", "SourceTableName": "", "Description": "", "Asms": "", "GdapArchLayer": "", "SourcingProject": "", "SubjectArea": "", "SourceDbName": "", "SourceSchemaName": "", "HiveSchema": "", "Location": "", "AutosysJobId": "", "CreatedBy": "", "CreatedDate": "", "UpdatedBy": "", "UpdatedDate": "", "ImportId": "", "TotalRecords": "", "TotalRecordsUpdatedDate": "" };

    this.subscription = this.selService.currentTable$.subscribe( 
      new_table_id => 
      {

        this.cur_table_id = this.selService.sel_table_id;
        this.cur_table = this.selService.sel_table;
      });



  }

  ngOnDestroy() {
  }




}
