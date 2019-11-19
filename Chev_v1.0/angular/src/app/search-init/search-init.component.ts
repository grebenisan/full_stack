
import { Component, OnInit } from '@angular/core';
// import { DataTableModule, SharedModule } from 'primeng/primeng';
import { SelectionService } from '../selection.service';
// import { Ifsentity } from '../ifsentity';
import { SearchCnt } from '../table';

export class Selopt {
  key: string;
  val: string;
}

@Component({
  selector: 'app-search-init',
  templateUrl: './search-init.component.html',
  styleUrls: ['./search-init.component.css']
})
export class SearchInitComponent implements OnInit {
  search_by: string;
  search_val: string;
  // meta_source: string;
  gdap_layer: string;
  // meta_source_val: string;
  // ifs_entities: Ifsentity[];
  // selectedIfsEntity: Ifsentity;

  // selectedIfs: string;
  // selectedEntity: string;
  // selectedHvSchema: string;
  // selectedEntSchema: string;
  cur_offset: number;
  page_size: number;

  cur_page_label: string;
  cur_search_cnt: string;

  running: boolean;

  curopts: Selopt[];
    loading: boolean;
/*
  ic_sel_options: Selopt[] =
  [
    {key: 'Project', val: 'proj' },
    {key: 'Subject area', val: 'subject_area' },
    {key: 'Hive schema', val: 'hv_schema' },
    {key: 'Autosys', val: 'autosys' },
    {key: 'IFS', val: 'ifs' },
    {key: 'Entity', val: 'entity' }
  ];

  ez_sel_options: Selopt[] =
  [
    {key: 'Project', val: 'proj' },
    {key: 'Subject area', val: 'subject_area' },
    {key: 'ASMS ID', val: 'asms_id' },
    {key: 'Source DB', val: 'source_db' },
    {key: 'Entity schema', val: 'ent_schema' },
    {key: 'Autosys', val: 'autosys' },
    {key: 'Application name', val: 'app_name' },
    {key: 'Entity', val: 'entity' }
  ];

  search_options: Selopt[] =
  [
    {key: 'Table Name', val: 'TableName' },
    {key: 'Table Business Name', val: 'BusinessName' },
    {key: 'Source Table Name', val: 'SourceTableName' },
    {key: 'ASMS', val: 'Asms' },
    {key: 'Sourcing Project', val: 'SourcingProject' },
    {key: 'Subject Area', val: 'SubjectArea' },
    {key: 'Source DB Name', val: 'SourceDbName' },
    {key: 'Source Schema Name', val: 'SourceSchemaName' },
    {key: 'Hive Schema', val: 'HiveSchema' },
    {key: 'Autosys Job Id', val: 'AutosysJobId' },
  ];
*/

  constructor(private selService: SelectionService) { }

  ngOnInit()
  {
    // this.meta_source_val = 'IC'; // depends on what the user selectes as the source of metadata
    // this.meta_source = 'ic';
    this.gdap_layer = 'IC';
    this.selService.cur_gdap_layer = this.gdap_layer;
    // this.selService.sel_meta_source = this.meta_source;
    this.cur_offset = 0;
    this.page_size = 200;

    this.cur_page_label = '1 to 200';
    this.cur_search_cnt = 'Unknown';

    this.running = false;


    // this.curopts = this.ic_sel_options;
    this.curopts = [
      {key: 'Table Name', val: 'TableName' },
      {key: 'Table Business Name', val: 'BusinessName' },
      {key: 'Source Table Name', val: 'SourceTableName' },
      {key: 'ASMS', val: 'Asms' },
      {key: 'Sourcing Project', val: 'SourcingProject' },
      {key: 'Subject Area', val: 'SubjectArea' },
      {key: 'Source DB Name', val: 'SourceDbName' },
      {key: 'Source Schema Name', val: 'SourceSchemaName' },
      {key: 'Hive Schema', val: 'HiveSchema' },
      {key: 'Autosys Job Id', val: 'AutosysJobId' },
    ];
  //  this.search_by = this.curopts[0].val;
  //  this.loading = true;
  //  setTimeout(() => { this.loading = false; }, 2000);
  //  this.getIfsEntities();
  //  this.loading = false;

  }


  onClickSearch() // click on the "Search" button
  {
    if (this.search_by === undefined || this.search_by === '')  {
      alert('Select a value for Search by!');
      return;
    }

    if (this.search_val === undefined || this.search_val === '') {
      alert('Enter a value for Search Value!');
      return;
    }

    this.selService.cur_gdap_layer = this.gdap_layer;
    this.selService.sel_search_by = this.search_by;
    this.selService.sel_search_val = this.search_val;

    this.selService.cur_offset = this.cur_offset;
    this.selService.page_size = this.page_size;

    this.cur_page_label = (this.cur_offset + 1).toString() + ' to ' + (this.cur_offset + this.page_size).toString();

    this.selService.getSearchCnt().subscribe(ret_serch_cnt => 
      {
        this.cur_search_cnt = ret_serch_cnt.total_cnt;
        this.cur_page_label = this.cur_page_label + ' out of ' + this.cur_search_cnt;
      });

    this.selService.triggerNewReset();
    this.selService.triggerNewSearch();
  }

  onClickNext()
  {
    if (this.search_by === undefined || this.search_by === '')  {
      alert('Select a value for Search by!');
      return;
    }

    if (this.search_val === undefined || this.search_val === '') {
      alert('Enter a value for Search Value!');
      return;
    }

    this.selService.cur_gdap_layer = this.gdap_layer;
    this.selService.sel_search_by = this.search_by;
    this.selService.sel_search_val = this.search_val;

    this.cur_offset = this.cur_offset + this.page_size;
    this.selService.cur_offset = this.cur_offset;
    this.selService.page_size = this.page_size;

    if (this.cur_search_cnt === undefined || this.cur_search_cnt === 'Unknown' || this.cur_search_cnt === '')
    {
      this.selService.getSearchCnt().subscribe(ret_serch_cnt => 
        {
          this.cur_search_cnt = ret_serch_cnt.total_cnt;
          this.cur_page_label = (this.cur_offset + 1).toString() + ' to ' + 
          (this.cur_offset + this.page_size).toString() + ' out of ' + this.cur_search_cnt;
        });
    } else
    {
      this.cur_page_label = (this.cur_offset + 1).toString() + ' to ' + 
      (this.cur_offset + this.page_size).toString() + ' out of ' + this.cur_search_cnt;
    }  

    this.selService.triggerNewReset();
    this.selService.triggerNewSearch();
  }

  onClickPrevious()
  {
    if (this.search_by === undefined || this.search_by === '')  {
      alert('Select a value for Search by!');
      return;
    }

    if (this.search_val === undefined || this.search_val === '') {
      alert('Enter a value for Search Value!');
      return;
    }

    this.selService.cur_gdap_layer = this.gdap_layer;
    this.selService.sel_search_by = this.search_by;
    this.selService.sel_search_val = this.search_val;

    this.cur_offset = this.cur_offset - this.page_size;
    if (this.cur_offset < 0)
    {
      this.cur_offset = 0;
    }
    this.selService.cur_offset = this.cur_offset;
    this.selService.page_size = this.page_size;

    if (this.cur_search_cnt === undefined || this.cur_search_cnt === 'Unknown' || this.cur_search_cnt === '')
    {
      this.selService.getSearchCnt().subscribe(ret_serch_cnt => 
        {
          this.cur_search_cnt = ret_serch_cnt.total_cnt;
          this.cur_page_label = (this.cur_offset + 1).toString() + ' to ' + 
          (this.cur_offset + this.page_size).toString() + ' out of ' + this.cur_search_cnt;
        });
    } else
    {
      this.cur_page_label = (this.cur_offset + 1).toString() + ' to ' + 
      (this.cur_offset + this.page_size).toString() + ' out of ' + this.cur_search_cnt;
    }  

    this.selService.triggerNewReset();
    this.selService.triggerNewSearch();
  }


  onSearchbyChange(new_val: string) // change of the select option from the drop-down box
  {
    // alert('current selection: ' + new_val); // it works
    this.search_val = '';
  }

  reset_object(): void
  {
    // this.selService.sel_meta_source = this.meta_source;
    this.selService.cur_gdap_layer = this.gdap_layer;
    // this.ifs_entities = []; // clear the list of entities
    // this.selectedIfsEntity = { ifs: '', entity: '', hv_schema: ''};
    // this.selectedIfs = '';
    // this.selectedEntity = '';
    // this.selectedHvSchema = '';
    // this.selectedEntSchema = '';

    this.selService.sel_entity = '';
    this.selService.sel_hv_schema = '';
    this.selService.sel_ifs = '';
    this.selService.sel_ent_schema = '';
  }
/*
  metasource_change(meta_src: string) 
  {

    if (meta_src === 'edge-base')
    {
      this.meta_source = 'edge-base';
      this.curopts = this.ez_sel_options;

    }

    if (meta_src === 'ic')
    {
      this.meta_source = 'ic';
      this.curopts = this.ic_sel_options;
    }

    this.reset_object();
  }
*/
  gdap_layer_change(layer: string)
  {
    if (layer === 'EDGE_BASE')
    {
      this.gdap_layer = 'EDGE_BASE';
      // this.curopts = this.ez_sel_options;

    }

    if (layer === 'IC')
    {
      this.gdap_layer = 'IC';
      // this.curopts = this.ic_sel_options;
    }

    this.reset_object();
  }

  page_size_change(new_size: number)
  {
    this.page_size = new_size;
    this.selService.page_size = this.page_size;
  }

}
