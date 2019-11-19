
import { Component, OnInit, OnDestroy } from '@angular/core';
// import { DataTableModule, SharedModule } from 'primeng/primeng';
import { DataTableModule } from 'primeng/datatable';
import { SharedModule } from 'primeng/shared';
// import {TableModule} from 'primeng/table';
import { Subscription } from 'rxjs/Subscription';

import { SelectionService } from '../selection.service';
import { Entitycol } from '../entitycol';
import { VEHALLIEDINV } from '../mock-entitycol';

import { Column } from '../column';
import { COLUMNLIST } from '../mock-entitycol';

@Component({
  selector: 'app-entity-def',
  templateUrl: './entity-def.component.html',
  styleUrls: ['./entity-def.component.css']
})
export class EntityDefComponent implements OnInit, OnDestroy {

 // entity_def_cols: Entitycol[];
 // sel_entity_cols: Entitycol;

  column_list: Column[];
  sel_column: Column;
  sel_column_id: string;

  cur_table_id: string;

  // sel_col_name: string;
  // sel_col_type: string;
  // sel_col_size: string;
  // cur_ifs: string;
  // cur_entity: string;
  // cur_hv_schema: string;
  // cur_ent_schema: string;
  loading: boolean;
  subscription: Subscription;
/*
  cols: any[] = [
    { field: 'col_name', header: 'Column name' },
    { field: 'data_type', header: 'Type' },
    { field: 'data_length', header: 'Size' },
    { field: 'is_pk', header: 'PK' },
    { field: 'is_fk', header: 'FK' },
    { field: 'is_null', header: 'Null' }
  ];
*/
  constructor(private selService: SelectionService) { }

  ngOnInit() {
    // initialize the members

    // this.entity_def_cols = VEHALLIEDINV; // just for testing 
    //// this.loading = true;
    //// setTimeout(() => { this.loading = false; }, 1000);
    this.loading = false;
    //// this.getEntityCols();
/*  
    this.subscription = this.selService.currentEntity$.subscribe(
      new_entity => {
        this.cur_entity = new_entity;
        this.cur_ifs = this.selService.sel_ifs;
        this.cur_hv_schema = this.selService.sel_hv_schema;
        this.cur_ent_schema = this.selService.sel_ent_schema;

        this.getEntityCols_local(); // testing mode 

        // production mode; call the right function depending on themeta_source
        if (this.selService.sel_meta_source === 'ic')
        {
          // this.getEntityCols(); // production mode
        }

        if (this.selService.sel_meta_source === 'edge-base')
        {
          // this.getEZEntityCols(); // the production mode
        }

    });
*/
    this.subscription = this.selService.currentTable$.subscribe(
      new_table_id =>
      {
        // this.cur_table_id = this.selService.sel_table_id;
        this.cur_table_id = new_table_id;
        // this.getListColumns_local(); // testing mode
        this.getListColumns(); // PROD mode
      });

      // this.column_list = COLUMNLIST;

  }

  ngOnDestroy() { this.subscription.unsubscribe(); }
/*
  handleRowSelect(event) {
    this.loading = false;
    this.sel_col_name = event.data.col_name;
    this.sel_col_type = event.data.data_type;
    this.sel_col_size = event.data.data_length;
    this.selService.sel_col_name = this.sel_col_name;
    this.selService.sel_col_type = this.sel_col_type;
    this.selService.sel_col_size = this.sel_col_size;

    this.selService.updateColumn(this.sel_col_name); // trigger the updateColumn event (currentColumnSource)
  }
*/
  handleColSelect(event) // latest
  {
    this.loading = false;
    this.selService.sel_column = this.sel_column;
    this.sel_column_id = event.data.Id;
    this.selService.sel_column_id = this.sel_column_id;

    this.selService.updateColumn(this.sel_column_id); // trigger the updateColumn event (currentColumnSource)
  }
/*
  getEntityCols_local(): void {
    this.selService.getEntityCols_local().subscribe(ret_entitycols => this.entity_def_cols = ret_entitycols);
  }
*/
/*
  getEntityCols(): void {  // this function must be streamlined (get rig of parameters, they are already available in the service)
    this.loading = true;
    this.selService.getEntityCols(this.cur_ifs, this.cur_entity, this.cur_hv_schema)
      .subscribe(ret_entitycols => { this.entity_def_cols = ret_entitycols; this.loading = false; });
    this.sel_col_name = '';
    // selected_entity_col = '{}'; to test this assignment
  }
*/
/*
  getEZEntityCols(): void {
    this.loading = true;
    this.selService.getEZEntityCols()
    .subscribe(ret_entitycols => { this.entity_def_cols = ret_entitycols; this.loading = false; });
    this.sel_col_name = '';
    // selected_entity_col = '{}'; to test this assignment
  }
*/
  getListColumns_local()
  {
    this.loading = true;
    this.selService.getListColumns_local().subscribe(
      ret_columns => 
      {
        this.column_list = ret_columns;
        this.loading = false;
      });

  }

  getListColumns()
  {
    this.loading = true;
    this.selService.getListColumns().subscribe(
      ret_columns => 
      {
        this.column_list = ret_columns;
        this.loading = false;
      });
  }

  reset_object(): void 
  {
    this.column_list = [];
    this.sel_column = { "Id": "", "ColumnName": "", "BusinessName": "", "SourceColumnName": "", "Description": "", "DataType": "", "DataLength": "", "IsNullable": "", "IsPrimaryKey": "", "IsForeignKey": "", "IsMultibyte": "", "IsCertified": "", "DataClassification": "", "UniqueRecords": "", "NullRecords": "", "DefaultRecords": "", "MinValue": "", "MaxValue": "", "MeanValue": "", "StDev": "", "MinLength": "", "MaxLength": "", "AvgLength": "", "StatsUpdatedDate": "" };
    this.selService.sel_column = this.sel_column;

  }

}

