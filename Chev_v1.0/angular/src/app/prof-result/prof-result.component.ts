
import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';

import { Profileresult } from '../profileresult';
import { MOCKPROFRESULT } from '../mock-profresult';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';


// import { Entitycol } from '../entitycol';


@Component({
  selector: 'app-prof-result',
  templateUrl: './prof-result.component.html',
  styleUrls: ['./prof-result.component.css']
})
export class ProfResultComponent implements OnInit, OnDestroy {

    profile_result: Profileresult;
    cur_profile_id: string;
    cur_colreg_id: string;

    //cur_meta_source: string;
    //cur_hv_schema: string;
    //cur_ifs: string;
    //cur_table: string;
    //cur_col_name: string;
    //cur_regexp_name: string;
    //cur_regexp_def: string;
    //cur_col_regexp_desc: string;
    //cur_col_id: string;
    //cur_regexp_id: string;
    //cur_dt_cq_id: string;

    // subscription_table: Subscription;
   
    sub_clear: Subscription;

    subscription: Subscription;

    constructor(private selService: SelectionService) { }

    ngOnInit() 
    {
      this.profile_result = { "record_count": "", "execution_date": "", "total_records": "", "unique_records": "", "column_count": "", "total_suspect_in_class": "", "default_count": "", "threshold_percent": "", "suspect_class_type": "", "length": "", "avg_length": "", "avg_value": "", "min_value": "", "max_value": "", "profile_id": "" };


        this.sub_clear = this.selService.currentTable$.subscribe(
          new_table_id =>
          {
            this.clearComponent();
          }
        );

        this.subscription = this.selService.currentColReg$.subscribe(
            new_colreg_id =>
            {
                this.cur_colreg_id = new_colreg_id;
                // this.getProfileResult_local(new_colreg_id); // local test mode
                this.getProfileResult(new_colreg_id); // production mode
            });

    }

    getProfileResult_local(colreg_id): void {
      this.cur_colreg_id = colreg_id;
        this.selService.getProfileResult_local().subscribe(ret_profresult => {
            this.profile_result = ret_profresult;
        });
    }

    getProfileResult(colreg_id: string): void 
    {
        this.cur_colreg_id = colreg_id;
        this.selService.getProfileResult(colreg_id).subscribe(ret_profresult => 
        {
            this.profile_result = ret_profresult;
            this.cur_profile_id = ret_profresult.profile_id;
        });

    }


    clearComponent(): void 
    {
        this.profile_result = {
            'record_count': '',
            'execution_date': '',
            'total_records': '',
            'unique_records': '',
            'column_count': '',
            'total_suspect_in_class': '',
            'default_count': '',
            'threshold_percent': '',
            'suspect_class_type': '',
            'length': '',
            'avg_length': '',
            'avg_value': '',
            'min_value': '',
            'max_value': '',
            'profile_id': '' };

        this.cur_profile_id = '';
        this.cur_colreg_id = '';

    }

    ngOnDestroy() { this.subscription.unsubscribe(); this.sub_clear.unsubscribe(); }

    test_init(): void {
    }

}
