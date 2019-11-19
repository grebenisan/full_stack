import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';

import { DataTableModule } from 'primeng/datatable';
import { SharedModule } from 'primeng/shared';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';

import { Regexp } from '../regexp';
import { RegExp } from '../regexp';

@Component({
  selector: 'app-regexp-list',
  templateUrl: './regexp-list.component.html',
  styleUrls: ['./regexp-list.component.css']
})
export class RegexpListComponent implements OnInit, OnDestroy {


  regexp_list: RegExp[];
  selected_regexp: RegExp;
  sel_regexp_id: string;
  
  loading: boolean;

  // cur_regexp_name: string;
  // cur_regexp: string;
  // cur_reg_exp_id: string;

  subscription: Subscription;
  

  constructor(private selService: SelectionService) { }

  ngOnInit() {

    this.selected_regexp = new RegExp;
    this.sel_regexp_id = '';

    // this.cur_regexp_name = '';
    // this.cur_regexp = '';
    // this.cur_reg_exp_id = '';

    this.loading = false;

    this.getRegexpList_local();

    this.subscription = this.selService.loadRegexpList$.subscribe(
      new_reexp_trig => {
        // this.getRegexpList(); // production mode
        this.getRegexpList_local(); // local testing
    });    
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }


  handleRowSelect(event)
  {
    this.loading = false;
    // this.cur_regexp_name = event.data.RegexpName;
    // this.cur_regexp = event.data.Regexp;
    // this.cur_reg_exp_id = event.data.RegexpId;

    this.selService.sel_regexp = this.selected_regexp; // the most important
    this.sel_regexp_id = event.data.RegexpId;
    this.selService.sel_regexp_id = this.sel_regexp_id;

    // this.selService.sel_regexp_id = this.cur_reg_exp_id;
    // this.selService.sel_regexp_name = this.cur_regexp_name;
    // this.selService.sel_regexp_def = this.cur_regexp;

    this.selService.updateRegexp(this.sel_regexp_id); // the selService function could use the local sel_regexp_id
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
/*
  decriptRegexpList(enc_regexp_list: Regexp[])
  {
    let i: number;
    for (i = 0; i < enc_regexp_list.length; i++)
    {
      enc_regexp_list[i].regexp = this.decriptRegexp(enc_regexp_list[i].regexp);
    }
  }
*/
decriptRegexpList(enc_regexp_list: RegExp[])
{
  let i: number;
  for (i = 0; i < enc_regexp_list.length; i++)
  {
    enc_regexp_list[i].Regexp = this.decriptRegexp(enc_regexp_list[i].Regexp);
  }
}


  getRegexpList_local()
  {
    this.loading = true;
    this.selService.getRegexpList_local().subscribe(ret_regexp_list => {
 
      this.decriptRegexpList(ret_regexp_list);
      this.regexp_list = ret_regexp_list;
      this.loading = false;
    });
  }  

}
