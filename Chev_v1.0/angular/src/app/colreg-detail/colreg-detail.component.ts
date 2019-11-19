import { Component, OnInit, OnDestroy } from '@angular/core';
import { SelectionService } from '../selection.service';
import { Subscription } from 'rxjs/Subscription';
import { Subscribable, Observable } from 'rxjs/Observable';
import { ColReg } from '../colregexp';
import { COLREGLIST } from '../mock-colregexp';

@Component({
  selector: 'app-colreg-detail',
  templateUrl: './colreg-detail.component.html',
  styleUrls: ['./colreg-detail.component.css']
})
export class ColregDetailComponent implements OnInit, OnDestroy {

  colreg: ColReg;
  // execution_date: string;
  // record_count: string;
  
  sub_clear: Subscription;

  subscription: Subscription;

  constructor(private selService: SelectionService) { }

  ngOnInit() {
    this.colreg = new ColReg;


    this.sub_clear = this.selService.currentTable$.subscribe(
      new_table_id =>
      {
        this.clearComponent();
      }
    );

    this.subscription = this.selService.currentColReg$.subscribe(
      new_colreg_id => {
        // this.getColRegDetails_local(new_colreg_id);
        this.getColRegDetails(new_colreg_id); // production mode
    });

  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
    this.sub_clear.unsubscribe();
  }

  getColRegDetails_local(new_colreg_id: string)
  { 
    this.selService.getColRegDetails(new_colreg_id).subscribe(ret_colreg =>
    {
      this.colreg = ret_colreg;

    });
  }

  getColRegDetails(new_colreg_id: string)
  {
    this.colreg = this.selService.sel_colreg;
  }

  clearComponent(): void
  {
    this.colreg = { 'ColumnName': '', 'RegexpName': '', 'ColRegName': '', 'Regexp': '', 'ColumnId': '', 'RegexpId': '', 'ColRegId': '' };
  }
  

}
