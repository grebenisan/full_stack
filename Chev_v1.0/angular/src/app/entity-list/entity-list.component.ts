
import { Component, OnInit } from '@angular/core';
import { DataTableModule } from 'primeng/datatable';
import { SharedModule } from 'primeng/shared';
// import { DataTableModule, SharedModule } from 'primeng/primeng';
// import { TableModule } from 'primeng/table';
import { Subscription } from 'rxjs/Subscription';

import { SelectionService } from '../selection.service';
// import { Ifsentity } from '../ifsentity';
import { Table } from '../table';

// import { IFSENTITIES } from '../mock-ifsentity'; // just for testing
import { TABLELIST } from '../mock-ifsentity'; // just for testing

@Component({
  selector: 'app-entity-list',
  templateUrl: './entity-list.component.html',
  styleUrls: ['./entity-list.component.css']
})
export class EntityListComponent implements OnInit {

  // ifs_entities: Ifsentity[];
  // selectedIfsEntity: Ifsentity;

  table_list: Table[];
  sel_table: Table;

  // selectedIfs: string;
  // selectedEntity: string;
  // selectedHvSchema: string;
  // selectedEntSchema: string;

  loading: boolean;
  subscription: Subscription;
  reset_subscription: Subscription;

/*
  cols: any[];

  ic_cols: any[] = [
    { field: 'entity', header: 'Entity' },
    { field: 'ifs', header: 'IFS' },
    { field: 'hv_schema', header: 'HV Schema' }
  ];

  ez_cols: any[] = [
    { field: 'entity', header: 'Entity' },
    { field: 'ifs', header: 'DB Schema' },
    { field: 'hv_schema', header: 'HV Schema' }
  ];
*/
  table_columns: any[] =
  [
    { field: 'TableName', header: 'Table Name' },
    { field: 'BusinessName', header: 'Business Name' },
    { field: 'SourceTableName', header: 'Source Table Name' },
    { field: 'Description', header: 'Description' },
    { field: 'Asms', header: 'Asms' },
    { field: 'GdapArchLayer', header: 'Gdap Arch Layer' },
    { field: 'SourcingProject', header: 'Sourcing Project' },
    { field: 'SubjectArea', header: 'Subject Area' },
    { field: 'SourceDbName', header: 'Source DB Name' },
    { field: 'SourceSchemaName', header: 'Source Schema Name' },
    { field: 'HiveSchema', header: 'Hive Schema' },
    { field: 'Location', header: 'Location' },
    { field: 'AutosysJobId', header: 'Autosys Job Id' },
    { field: 'CreatedBy', header: 'Created By' },
    { field: 'CreatedDate', header: 'Created Date' },
    { field: 'UpdatedBy', header: 'Updated By' },
    { field: 'UpdatedDate', header: 'Updated Date' },
    { field: 'ImportId', header: 'Import Id' },
    { field: 'TotalRecords', header: 'Total Records' },
    { field: 'TotalRecordsUpdatedDate', header: 'Total Records Updated Date' },
    { field: 'Id', header: 'Id' }
  ];


  constructor(private selService: SelectionService) { }

  ngOnInit()
  {
    this.loading = false;

      // this.cols = this.ic_cols;
      // this.ifs_entities = IFSENTITIES;

    this.subscription = this.selService.newSearch$.subscribe(
      search_trigger => {
        // this.loading = true;
        // this.getListEntities_local(); // testing mode
        // this.getListEntities(); // production mode
        // this.getListTables_local(); // latest
        this.getListTables(); // latest prod mode
    });

    this.reset_subscription = this.selService.resetTrigger$.subscribe(
      reset_trigger => {
        // alert('reset event: ' + String(reset_trigger));
        this.reset_object();
      }
    );

  }
/*
  getListEntities_local(): void 
  {
    this.loading = true;
    this.selService.getListEntities_local().subscribe(ret_entities => {
      this.ifs_entities = ret_entities;
      this.loading = false;
    });
  }


  getListEntities(): void {
    this.loading = true;
    this.selService.getListEntities().subscribe(ret_entities => {
      this.ifs_entities = ret_entities;
      this.loading = false;
    }); // to be continued
  }
*/
  getListTables_local(): void {
    this.loading = true;
    this.selService.getListTables_local().subscribe(ret_tables => {
      this.table_list = ret_tables;
      this.loading = false;
    });
  }

  getListTables(): void {
    this.loading = true;
    this.selService.getListTables().subscribe(ret_tables => {
      this.table_list = ret_tables;
      this.loading = false;
    });
  }

/*
  handleRowSelect(event)
  {
    this.loading = false;

    // alert(event.data.entity);
    this.selectedEntity = event.data.entity;
    this.selectedHvSchema = event.data.hv_schema;
    // this.selectedIfs = this.selectedIfsEntity.ifs; -- this works too
    // this.selectedEntity = this.selectedIfsEntity.entity; -- this works too

    this.selService.sel_entity = event.data.entity;
    this.selService.sel_hv_schema = event.data.hv_schema;

    if ( this.selService.cur_gdap_layer === 'IC')
    {
      this.selectedIfs = event.data.ifs;
      this.selectedEntSchema = '';
      //this.selService.sel_table = 'IC_' + this.selectedEntity;
    }

    if (this.selService.cur_gdap_layer === 'EDGE_BASE')
    {
      this.selectedIfs = '';
      this.selectedEntSchema = event.data.ifs;
      //this.selService.sel_table = this.selectedEntity;
    }

    this.selService.sel_ifs = this.selectedIfs;
    this.selService.sel_ent_schema = this.selectedEntSchema;

    // update the observable stream of the service
    this.selService.updateEntity(this.selectedEntity);
  }
*/

  handleTableSelect(event)
  {
    this.loading = false;
    this.selService.sel_table = this.sel_table;
    this.selService.sel_table_id = event.data.Id;

    this.selService.updateTable(event.data.Id);
  }



/*
  getIfsEntities(sby: string, sval: string): void
  {
    // this.selService.getIfsEntities().subscribe(ret_ifstables => this.ifs_entities = ret_ifstables);
    // this.selService.getTest1().subscribe(ret_ifstables => this.ifs_entities = ret_ifstables);
    this.loading = true;
    this.selService.getIfsEntities(sby, sval).subscribe(ret_ifstables => {this.ifs_entities = ret_ifstables; this.loading = false; });
  }

  getIfsEntities_local(): void 
  {
    this.loading = true;
    this.selService.getIfsEntities_local().subscribe(ret_ifstables => {this.ifs_entities = ret_ifstables; this.loading = false; });
  }

  getEZEntities(sby: string, sval: string): void 
  {
    // this.selService.getIfsEntities().subscribe(ret_ifstables => this.ifs_entities = ret_ifstables);
    // this.selService.getTest1().subscribe(ret_ifstables => this.ifs_entities = ret_ifstables);
    this.loading = true;
    this.selService.getEZEntities(sby, sval).subscribe(ret_ifstables => {this.ifs_entities = ret_ifstables; this.loading = false; });
  }

  getEZEntities_local(): void 
  {
    this.loading = true;
    this.selService.getEZEntities_local().subscribe(ret_eztables => {
      this.ifs_entities = ret_eztables;
      this.loading = false;
    });
  }
*/
  reset_object(): void 
  {

    this.table_list = []; // clear the list of entities
    //this.selectedIfsEntity = { ifs: '', entity: '', hv_schema: ''};
    this.sel_table = { "Id": "", "TableName": "", "BusinessName": "", "SourceTableName": "", "Description": "", "Asms": "", "GdapArchLayer": "", "SourcingProject": "", "SubjectArea": "", "SourceDbName": "", "SourceSchemaName": "", "HiveSchema": "", "Location": "", "AutosysJobId": "", "CreatedBy": "", "CreatedDate": "", "UpdatedBy": "", "UpdatedDate": "", "ImportId": "", "TotalRecords": "", "TotalRecordsUpdatedDate": "" }; 
    this.selService.sel_table = this.sel_table;
  }


}


