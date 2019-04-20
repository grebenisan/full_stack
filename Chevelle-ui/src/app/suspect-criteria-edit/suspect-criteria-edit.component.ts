import { Component, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';
import { ChevUiService } from '../chev-ui.service';

import { DatePipe } from '@angular/common';

import { SuspectCriteria, SUSPECT_CRITERIA_EMPTY } from '../suspect-criteria';
import { DcName } from '../dataclass';

//export class DcName {
//  dc_name: string;
//  dc_val: string;
//}

export class SqlOps {
  sql_name: string;
  sql_val: string;
}


@Component({
  selector: 'app-suspect-criteria-edit',
  templateUrl: './suspect-criteria-edit.component.html',
  styleUrls: ['./suspect-criteria-edit.component.css']
})
export class SuspectCriteriaEditComponent implements OnInit {

  sc_cur_rec: SuspectCriteria;  // the suspect criteria current record being processed
  sc_temp_rec: SuspectCriteria; // the suspect criteria temporary record that is edited before saving or canceling
  edit_disable: boolean;
  update_disable: boolean;
  cancel_disable: boolean;
  delete_disable: boolean;
  new_disable: boolean;
  new_save_disable: boolean;
  new_cancel_disable: boolean;

  edit_controls_disable: boolean;


  delete_dialog: boolean;

  running: boolean;

  dc_names: DcName[];
  sql_match_ops: SqlOps[];

  // sel_dc_name: string;
  // sel_sql_match_op: string;

  sc_data_subscription: Subscription;   // suspect criteria data is available (has returned) from the microservice
  sc_selection_subscription: Subscription; // a new suspect criteria record was selected
  sc_update_subscription: Subscription;


  constructor( private chevUiService: ChevUiService, private datepipe: DatePipe ) { }

  ngOnInit() {

    this.sc_cur_rec = SUSPECT_CRITERIA_EMPTY;
    this.sc_temp_rec = new SuspectCriteria();

    this.sc_temp_rec.data_cls_nm = '';
    this.sc_temp_rec.match_type_opr = '';
    this.sc_temp_rec.match_nm = '';
    this.sc_temp_rec.upd_by = '';
    this.sc_temp_rec.upd_ts = '';
    this.sc_temp_rec.upd_rsn_txt = '';
    this.sc_temp_rec.id = 0;

    this.edit_disable = true;
    this.update_disable = true;
    this.cancel_disable = true;
    this.delete_disable = true;
    this.new_disable = true;
    this.new_save_disable = true;
    this.new_cancel_disable = true;

    this.edit_controls_disable = true;

    this.delete_dialog = false;

    this.running = false;
    // this.sel_dc_name = '';
    // this.sel_sql_match_op = '';
    this.dc_names = this.chevUiService.dc_name_list;

    this.sql_match_ops = [
      // {sql_name: 'like', sql_val: 'like' },
      {sql_name: 'not like', sql_val: 'not like' },
      {sql_name: 'regexp_like', sql_val: 'regexp_like' },
      {sql_name: 'not regexp_like', sql_val: 'not regexp_like' },
      {sql_name: 'in', sql_val: 'in' },
      {sql_name: 'not in', sql_val: 'not in' },
      {sql_name: '=', sql_val: '=' }
    ];

    // suspect criteria data is available (has returned) from the microservice
    this.sc_data_subscription = this.chevUiService.obsvScInformSearch$.subscribe( sc_inform_ret => {

      // alert('Suspect Criteria Edit: got a new SC data avilable trigger: ' + sc_inform_ret.toString())

      // enable the Edit, Delete and New buttons
      this.edit_disable = true;
      this.update_disable = true;
      this.cancel_disable = true;
      this.delete_disable = true;
      this.new_disable = true;
      this.new_save_disable = true;
      this.new_cancel_disable = true;

      this.edit_controls_disable = true;
      this.dc_names = this.chevUiService.dc_name_list;

      this.sc_cur_rec = SUSPECT_CRITERIA_EMPTY;  // the suspect criteria current record being processed
      this.sc_temp_rec = {
        'data_cls_nm': '',
        'match_type_opr': '',
        'match_nm': '',
        'upd_by': '',
        'upd_ts': '',
        'upd_rsn_txt': '',
        'id': 0,
      };

    });

    // a suspect criteria record was selected
    this.sc_selection_subscription = this.chevUiService.obsvScInformSelection$.subscribe( sc_select_ret => {
      // alert('Suspect Criteria Edit: new record selection: ' + this.chevUiService.sel_sc_rec.match_nm.toString());

      this.edit_disable = false;
      this.update_disable = true;
      this.cancel_disable = true;
      this.delete_disable = false;
      this.new_disable = false;
      this.new_save_disable = true;
      this.new_cancel_disable = true;

      this.edit_controls_disable = true;

      this.sc_cur_rec = this.chevUiService.sel_sc_rec;
      this.updateScTempRec();
    });

    this.sc_update_subscription = this.chevUiService.obsvScInformUpdate$.subscribe( sc_update_ret => {
      // enable the Edit, Delete and New buttons
      this.edit_disable = true;
      this.update_disable = true;
      this.cancel_disable = true;
      this.delete_disable = true;
      this.new_disable = true;
      this.new_save_disable = true;
      this.new_cancel_disable = true;

      this.edit_controls_disable = true;
      this.dc_names = this.chevUiService.dc_name_list;

    });

  } // onInit()

  // function to set the controls of the Edit component, with the values of the selected suspect criteria record
  onEdit()
  {
    // alert('Suspect Criteria Edit: Edit suspect criteria: ' + this.sc_cur_rec.match_nm);
    this.edit_disable = true;
    this.update_disable = false;
    this.cancel_disable = false;
    this.delete_disable = true;
    this.new_disable = true;
    this.new_save_disable = true;
    this.new_cancel_disable = true;

    this.edit_controls_disable = false;
  }

  onUpdate() // updating a record that was just edited
  {
    this.running = true;
    // first update the temp record - upd by, upd ts, reason
    let objDate = new Date();
    this.sc_temp_rec.upd_by = 'chevelle-ui'; // to be changed with the user ID after the authentication
    this.sc_temp_rec.upd_ts = this.datepipe.transform(objDate, 'yyyy-MM-dd HH:mm:ss');
    this.sc_temp_rec.upd_rsn_txt = 'updated';

    // console.log(this.sc_temp_rec.upd_ts);
    // call the Angular Service with the new values for the current index
    this.chevUiService.updateScListValue(this.sc_temp_rec);

    // update the current selected SC record
    this.sc_cur_rec = this.cloneSuspectCriteria(this.sc_temp_rec);
    this.chevUiService.sel_sc_rec = this.cloneSuspectCriteria(this.sc_temp_rec);

    this.chevUiService.httpUpdateSuspectCriteria(this.sc_cur_rec).subscribe(upd_sc_ret => {
      this.chevUiService.informNewUpdateSuspectCriteria('update');
      // refresh the list component
      this.edit_disable = false;
      this.update_disable = true;
      this.cancel_disable = true;
      this.delete_disable = false;
      this.new_disable = false;
      this.new_save_disable = true;
      this.new_cancel_disable = true;
  
      this.edit_controls_disable = true;
      this.running = false;
    });

  }

  onCancel()
  {
    // update the current list component with the buffer vale
    this.restoreCurSelScRec();

    this.edit_disable = false;
    this.update_disable = true;
    this.cancel_disable = true;
    this.delete_disable = false;
    this.new_disable = false;
    this.new_save_disable = true;
    this.new_cancel_disable = true;

    this.edit_controls_disable = true;
  }

  cloneScTempRec()
  {
    this.sc_temp_rec = new SuspectCriteria();
    this.sc_temp_rec.data_cls_nm = this.sc_cur_rec.data_cls_nm;
    this.sc_temp_rec.match_type_opr = this.sc_cur_rec.match_type_opr;
    this.sc_temp_rec.match_nm = this.sc_cur_rec.match_nm;
    this.sc_temp_rec.id = this.sc_cur_rec.id;
  }

  restoreCurSelScRec()
  {
    
    this.sc_temp_rec.data_cls_nm = this.sc_cur_rec.data_cls_nm;
    this.sc_temp_rec.match_type_opr = this.sc_cur_rec.match_type_opr;
    this.sc_temp_rec.match_nm = this.sc_cur_rec.match_nm;
    this.sc_temp_rec.id = this.sc_cur_rec.id;
  }

  cloneSuspectCriteria(sc: SuspectCriteria): SuspectCriteria
  {
    let cloned_sc = new SuspectCriteria();

    cloned_sc.data_cls_nm = sc.data_cls_nm;
    cloned_sc.match_type_opr = sc.match_type_opr;
    cloned_sc.match_nm = sc.match_nm;
    cloned_sc.upd_by = sc.upd_by;
    cloned_sc.upd_ts = sc.upd_ts;
    cloned_sc.upd_rsn_txt = sc.upd_rsn_txt;
    cloned_sc.id = sc.id;

    return cloned_sc;
  }

  updateScTempRec()
  {
    this.sc_temp_rec.data_cls_nm = this.sc_cur_rec.data_cls_nm;
    this.sc_temp_rec.match_type_opr = this.sc_cur_rec.match_type_opr;
    this.sc_temp_rec.match_nm = this.sc_cur_rec.match_nm;
    this.sc_temp_rec.upd_by = this.sc_cur_rec.upd_by;
    this.sc_temp_rec.upd_ts = this.sc_cur_rec.upd_ts;
    this.sc_temp_rec.upd_rsn_txt = this.sc_cur_rec.upd_rsn_txt;
    this.sc_temp_rec.id = this.sc_cur_rec.id;

  }

  onSelDcNameChange(new_dc_name: string)
  {
    // alert('Selected DC name: ' + new_dc_name);
  }

  onSelSqlMatchOpChange(new_sql_match_op: string)
  {
    // alert('Selected SQL operator: ' + new_sql_match_op);
  }

  onDelete()
  {
    // pop-up a modal dialog: "Are you sure ...", Yes, Cancel
    this.delete_dialog = true;
  }


  onNew()
  {
    
    // enable the edit fields above
    // this.sc_cur_rec = new SuspectCriteria();
    this.sc_temp_rec = new SuspectCriteria();

    // enable the Save and Cancel buttons
    this.edit_disable = true;
    this.update_disable = true;
    this.cancel_disable = true;
    this.delete_disable = true;
    this.new_disable = true;
    this.new_save_disable = false;
    this.new_cancel_disable = false;

    this.edit_controls_disable = false;

  }

  onNewSave() // saving a newly created suspect criteria
  {
    
    this.running = true;

    // first update the temp record - upd by, upd ts, reason
    let objDate = new Date();
    this.sc_temp_rec.upd_by = 'chevelle-ui';
    this.sc_temp_rec.upd_ts = this.datepipe.transform(objDate, 'yyyy-MM-dd HH:mm:ss');
    this.sc_temp_rec.upd_rsn_txt = 'updated';

    // insert the record in to the list
    // let new_sc = new SuspectCriteria();

  /*
    new_sc.data_cls_nm = this.sc_temp_rec.data_cls_nm;
    new_sc.match_type_opr = this.sc_temp_rec.match_type_opr;
    new_sc.match_nm = this.sc_temp_rec.match_nm;
    new_sc.upd_by = this.sc_temp_rec.upd_by;
    new_sc.upd_ts = this.sc_temp_rec.upd_ts;
    new_sc.upd_rsn_txt = this.sc_temp_rec.upd_rsn_txt;
    new_sc.id = this.chevUiService.findMaximRowIdNext();
*/
    this.sc_temp_rec.id = this.chevUiService.findMaximRowIdNext();

    this.chevUiService.addNewSuspectCriteria(this.sc_temp_rec);   

    // update the current selected SC record
    this.sc_cur_rec = this.cloneSuspectCriteria(this.sc_temp_rec);
    this.chevUiService.sel_sc_rec = this.cloneSuspectCriteria(this.sc_temp_rec);

    this.chevUiService.httpUpdateSuspectCriteria(this.sc_cur_rec).subscribe(upd_sc_ret => { 

      this.chevUiService.informNewUpdateSuspectCriteria('newsave');


      // enable the Save and Cancel buttons
      this.edit_disable = false;
      this.update_disable = true;
      this.cancel_disable = true;
      this.delete_disable = false;
      this.new_disable = false;
      this.new_save_disable = true;
      this.new_cancel_disable = true;
      // inform the controls
      this.edit_controls_disable = true;
      this.running = false;
    });

    // this.chevUiService.informNewListSuspectCriteria();

  }

  onNewCancel()
  {
    // update the controls with the selected record
    this.restoreCurSelScRec();
    // update the disbled stats of the buttons and controls

    // enable the Save and Cancel buttons
    this.edit_disable = false;
    this.update_disable = true;
    this.cancel_disable = true;
    this.delete_disable = false;
    this.new_disable = false;
    this.new_save_disable = true;
    this.new_cancel_disable = true;

    this.edit_controls_disable = true;
  }
  
  
  // this is the Delete action of the modal dialog box
  deleteCurrentSc()
  {
    this.delete_dialog = false;
    this.running = true;

    let del_sc_copy = this.cloneSuspectCriteria(this.sc_cur_rec);
    // do the delete; remove the record from the array
    this.chevUiService.deleteSuspectCriteria(this.sc_cur_rec);

    // reset the current controls
    this.sc_cur_rec = SUSPECT_CRITERIA_EMPTY;
    this.sc_temp_rec = new SuspectCriteria();

    this.chevUiService.httpDeleteSuspectCriteria(del_sc_copy).subscribe( del_sc_ret => {
      // refresh the list component
      this.chevUiService.informNewUpdateSuspectCriteria('delete');
      this.running = false;
      del_sc_copy = null;
    });



  }

  // this is the Cancel action of the modal dialog box
  cancelCurrentSc()
  {
    // do the cancel
    this.delete_dialog = false;

  }

  enableDataClassName(): boolean
  {
    return (this.update_disable || this.new_save_disable);
  }

}

