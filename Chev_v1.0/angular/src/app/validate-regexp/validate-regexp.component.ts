import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';

import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';
import { TimerObservable } from 'rxjs/observable/TimerObservable';

import {ProgressBarModule} from 'primeng/progressbar';



@Component({
  selector: 'app-validate-regexp',
  templateUrl: './validate-regexp.component.html',
  styleUrls: ['./validate-regexp.component.css']
})
export class ValidateRegexpComponent implements OnInit, OnDestroy {

  user_regexp: string;
  sel_regexp: string;
  sel_regexp_def: string;
  sel_regexp_name: string;
  sel_regexp_id: string;
  current_regexp: string;
  sample_data: string;
  valid_data: string;
  radio_selection: string;

  current_server: string;
  loading: boolean;

  regexp_subscription: Subscription; // subscription for new regexp event
  validate_subscription: Subscription; // subscription for the Validate action


  constructor(private selService: SelectionService) { }

  ngOnInit() {
    this.user_regexp = '';
    this.sel_regexp_def = '';
    this.radio_selection = 'user-regexp';
    this.sample_data = '';
    this.current_server = 'p1';
    this.loading = false;

    this.regexp_subscription = this.selService.currentRegexp$.subscribe(new_regexp_id => {
      // this.sel_regexp_name = this.selService.sel_regexp_name;
      // this.sel_regexp_def = this.selService.sel_regexp_def;
      // this.sel_regexp_id = this.selService.sel_regexp_id;

      this.sel_regexp_name = this.selService.sel_regexp.RegexpName;
      this.sel_regexp_def = this.selService.sel_regexp.Regexp;
      this.sel_regexp_id = this.selService.sel_regexp.RegexpId;

      this.current_regexp = this.sel_regexp_def;
    });
  }

  ngOnDestroy() {
    this.regexp_subscription.unsubscribe();
  }

  Select_server(cur_server: string)
  {
    this.current_server = cur_server;
  }

  ValidateRegexpData()
  {

    // find what is the current user selection: user_regexp or selected_regexp
    if (this.radio_selection === 'user-regexp')
    {
      if (this.user_regexp === '')
      {
        alert('Please enter a valid regular expression before testing!');
        return;
      } else {
        this.current_regexp = this.user_regexp;
      }
    }

    if (this.radio_selection === 'sel-regexp')
    {
      if (this.sel_regexp_def === '')
      {
        alert('Please select a regular expression that has a valid definition!');
        // alert('Current regexp is: ' + this.sel_regexp_def);
        return;
      } else {
        this.current_regexp = this.sel_regexp_def;
      }
    }

    if (this.sample_data === '')
    {
      alert('Please enter a valid data set to test your regular expression!');
      return;
    }

    // at this point we have at least one selection with a valid regular expression
    // proceed with processing (no need for else above)
    this.selService.triggerValidateRegexp(); // probably not needed

    this.loading = true;

    this.valid_data = '';

    this.selService.runValidateRegexp(this.sample_data, this.current_regexp, this.current_server).subscribe(val_return =>
    {
      this.valid_data = '';
      for (let val_row of val_return)
      {
        this.valid_data += val_row.r + '\n';
      }
      this.loading = false;
    });

  }

  Select_regexp_change(regexp_selection)
  {
    if (regexp_selection === 'user-regexp')
    {
      this.radio_selection = 'user-regexp';
    }

    if (regexp_selection === 'sel-regexp')
    {
      this.radio_selection = 'sel-regexp';
    }

  }  


}
